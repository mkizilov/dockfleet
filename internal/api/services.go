package api

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Service definitions are desired state: the API records what an administrator
// wants and nothing here places, schedules or starts anything.

// serviceView is a service as the API returns it.
type serviceView struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`

	// Revision is the deployment revision currently wanted; Spec is its content.
	Revision int               `json:"revision"`
	Spec     store.ServiceSpec `json:"spec"`

	// Rollout is how a change of spec replaces the running replicas. Changing it
	// starts no deployment.
	Rollout store.RolloutPolicy `json:"rollout"`

	// Version changes with every modification and is the ETag.
	Version int64 `json:"version"`

	// Status is what the scheduler has decided about the service's replicas.
	// It is read-only: scheduling is not something a definition can ask for.
	Status serviceStatus `json:"status"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func newServiceView(svc store.Service, spec store.ServiceSpec, status serviceStatus) serviceView {
	return serviceView{
		ID: svc.ID, Name: svc.Name, Replicas: svc.Replicas, Revision: svc.CurrentRevision, Version: svc.Version,
		Spec: spec, Rollout: svc.Rollout, Status: status, CreatedAt: svc.CreatedAt, UpdatedAt: svc.UpdatedAt, DeletedAt: svc.DeletedAt,
	}
}

func etag(version int64) string {
	return `"` + strconv.FormatInt(version, 10) + `"`
}

func writeService(w http.ResponseWriter, code int, v serviceView) {
	w.Header().Set("ETag", etag(v.Version))
	writeJSON(w, code, v)
}

func writeProblems(w http.ResponseWriter, probs problems) {
	writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid service definition", "details": probs})
}

// readServiceInput reads and validates a request body. It has answered the
// request already when ok is false.
func readServiceInput(w http.ResponseWriter, r *http.Request, creating bool, pathName string) (in serviceInput, ok bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, "the request body is too large")
		return in, false
	case err != nil:
		writeError(w, http.StatusBadRequest, "the request body could not be read")
		return in, false
	}
	in, probs := parseService(body, creating, pathName)
	if len(probs) > 0 {
		writeProblems(w, probs)
		return in, false
	}
	return in, true
}

// readIfMatch returns the version the client says it read, or 0 for none. It
// has answered the request already when ok is false.
func readIfMatch(w http.ResponseWriter, r *http.Request) (version int64, ok bool) {
	version, ok = parseIfMatch(r.Header.Get("If-Match"))
	if !ok {
		writeError(w, http.StatusBadRequest, `If-Match must be the quoted version from the ETag header, for example "3", or *`)
	}
	return version, ok
}

// requestError is a refusal with its own status and words, for the cases a
// store error does not describe: a rollback with nothing to go back to, a
// revision that does not exist, a rollout policy that cannot be used.
type requestError struct {
	status  int
	message string
	// problems, when set, are reported as the details of a 400.
	problems problems
}

func (e requestError) Error() string { return e.message }

// badRollout is the refusal of a rollout policy that parses but cannot be used
// once it is merged with the one the service has.
func badRollout(err error) requestError {
	message := strings.TrimPrefix(err.Error(), store.ErrInvalid.Error()+": ")
	return requestError{
		status: http.StatusBadRequest, message: "invalid service definition",
		problems: problems{{Field: "rollout", Message: message}},
	}
}

// failService answers a failed service operation. precondition is whether the
// client sent an If-Match, which decides how a lost race is reported.
func (s *server) failService(w http.ResponseWriter, r *http.Request, err error, precondition bool) {
	var refusal requestError
	switch {
	case errors.As(err, &refusal) && refusal.problems != nil:
		writeJSON(w, refusal.status, map[string]any{"error": refusal.message, "details": refusal.problems})
	case errors.As(err, &refusal):
		writeError(w, refusal.status, refusal.message)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "service not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "a service with this name already exists")
	case errors.Is(err, store.ErrVersionConflict) && precondition:
		writeError(w, http.StatusPreconditionFailed, "the service was changed since you read it; read it again and retry")
	case errors.Is(err, store.ErrVersionConflict):
		writeError(w, http.StatusConflict, "the service was changed at the same time; retry")
	default:
		s.internalError(w, r, err)
	}
}

func (s *server) createService(w http.ResponseWriter, r *http.Request) {
	in, ok := readServiceInput(w, r, true, "")
	if !ok {
		return
	}
	policy := in.rollout.apply(store.DefaultRolloutPolicy())
	if err := policy.Validate(); err != nil {
		s.failService(w, r, badRollout(err), false)
		return
	}
	id, now := store.NewID(), s.now().UTC()
	var svc store.Service
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		var err error
		svc, err = tx.CreateService(r.Context(), store.NewService{
			ID: id, Name: in.name, Replicas: in.replicas, Spec: in.spec, Rollout: policy, CreatedBy: adminActor, At: now,
		})
		if err != nil {
			return err
		}
		_, err = tx.AppendEvent(r.Context(), s.event("service.created", "service", id, "", map[string]string{
			"name": in.name, "replicas": strconv.Itoa(in.replicas), "image": in.spec.Image, "revision": "1",
		}))
		return err
	})
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	status, err := s.statusOf(r.Context(), svc)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/services/"+svc.Name)
	writeService(w, http.StatusCreated, newServiceView(svc, in.spec, status))
}

func (s *server) listServices(w http.ResponseWriter, r *http.Request) {
	includeDeleted := false
	if raw := r.URL.Query().Get("include_deleted"); raw != "" {
		var err error
		if includeDeleted, err = strconv.ParseBool(raw); err != nil {
			writeError(w, http.StatusBadRequest, "include_deleted must be true or false")
			return
		}
	}
	defs, err := s.store.ListDefinitions(r.Context(), includeDeleted)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	statuses, err := s.statusesOf(r.Context(), defs)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	views := make([]serviceView, 0, len(defs))
	for _, d := range defs {
		views = append(views, newServiceView(d.Service, d.Revision.Spec, statuses[d.Service.ID]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": views})
}

// liveService finds the service the URL names. Names are unique among live
// services only, so a deleted service is not addressable by name.
func (s *server) liveService(r *http.Request, q store.ServiceRepository) (store.Service, store.Revision, error) {
	svc, err := q.GetServiceByName(r.Context(), r.PathValue("name"))
	if err != nil {
		return store.Service{}, store.Revision{}, err
	}
	rev, err := q.GetRevision(r.Context(), svc.ID, svc.CurrentRevision)
	return svc, rev, err
}

func (s *server) getService(w http.ResponseWriter, r *http.Request) {
	svc, rev, err := s.liveService(r, s.store)
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	status, err := s.statusOf(r.Context(), svc)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeService(w, http.StatusOK, newServiceView(svc, rev.Spec, status))
}

// updateService replaces the definition: the replica count and the spec are
// set to the request's. A changed spec becomes a new deployment revision, a
// changed replica count a scaling; an identical definition changes nothing and
// records nothing. With If-Match the update only applies to the version the
// client read.
func (s *server) updateService(w http.ResponseWriter, r *http.Request) {
	want, ok := readIfMatch(w, r)
	if !ok {
		return
	}
	in, ok := readServiceInput(w, r, false, r.PathValue("name"))
	if !ok {
		return
	}
	var out store.Service
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx := r.Context()
		svc, cur, err := s.liveService(r, tx)
		if err != nil {
			return err
		}
		if want != 0 && svc.Version != want {
			return store.ErrVersionConflict
		}
		now, expected := s.now().UTC(), want

		if in.rollout != nil {
			next := in.rollout.apply(svc.Rollout)
			if err := next.Validate(); err != nil {
				return badRollout(err)
			}
			svc, err = tx.SetRollout(ctx, svc.ID, next, svc.Paused, expected, now)
			switch {
			case errors.Is(err, store.ErrUnchanged):
			case err != nil:
				return err
			default:
				expected = svc.Version
				_, err = tx.AppendEvent(ctx, s.event("service.rollout_configured", "service", svc.ID, "", map[string]string{
					"name": svc.Name, "max_unavailable": strconv.Itoa(next.MaxUnavailable), "max_surge": strconv.Itoa(next.MaxSurge),
					"progress_deadline_seconds": strconv.Itoa(next.ProgressDeadlineSeconds),
				}))
				if err != nil {
					return err
				}
			}
		}

		if cur.SpecHash != in.spec.Hash() {
			var rev store.Revision
			svc, rev, err = tx.DeployService(ctx, svc.ID, store.Deployment{Spec: in.spec, CreatedBy: adminActor, ExpectedVersion: expected, At: now})
			switch {
			case errors.Is(err, store.ErrUnchanged):
				// Another manager deployed this very spec first; nothing left to do.
			case err != nil:
				return err
			default:
				expected = svc.Version
				_, err = tx.AppendEvent(ctx, s.event("service.deployed", "service", svc.ID, "", map[string]string{
					"name": svc.Name, "from_revision": strconv.Itoa(cur.Revision), "to_revision": strconv.Itoa(rev.Revision), "image": in.spec.Image,
				}))
				if err != nil {
					return err
				}
			}
		}
		if in.replicas != svc.Replicas {
			from := svc.Replicas
			svc, err = tx.ScaleService(ctx, svc.ID, in.replicas, expected, now)
			switch {
			case errors.Is(err, store.ErrUnchanged):
			case err != nil:
				return err
			default:
				_, err = tx.AppendEvent(ctx, s.event("service.scaled", "service", svc.ID, "", map[string]string{
					"name": svc.Name, "from_replicas": strconv.Itoa(from), "to_replicas": strconv.Itoa(in.replicas),
				}))
				if err != nil {
					return err
				}
			}
		}
		out = svc
		return nil
	})
	if err != nil {
		s.failService(w, r, err, want != 0)
		return
	}
	status, err := s.statusOf(r.Context(), out)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeService(w, http.StatusOK, newServiceView(out, in.spec, status))
}

// deleteService marks the service deleted and its desired replicas zero. Its
// revisions and history are kept, and its name can be used again.
func (s *server) deleteService(w http.ResponseWriter, r *http.Request) {
	want, ok := readIfMatch(w, r)
	if !ok {
		return
	}
	var deleted store.Service
	var spec store.ServiceSpec
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		svc, cur, err := s.liveService(r, tx)
		if err != nil {
			return err
		}
		if deleted, err = tx.DeleteService(r.Context(), svc.ID, want, s.now().UTC()); err != nil {
			return err
		}
		spec = cur.Spec
		_, err = tx.AppendEvent(r.Context(), s.event("service.deleted", "service", svc.ID, "", map[string]string{"name": svc.Name}))
		return err
	})
	if err != nil {
		s.failService(w, r, err, want != 0)
		return
	}
	// The workloads the service already has are kept, and still shown.
	status, err := s.statusOf(r.Context(), deleted)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeService(w, http.StatusOK, newServiceView(deleted, spec, status))
}

// listServiceRevisions returns the deployment history, oldest first.
func (s *server) listServiceRevisions(w http.ResponseWriter, r *http.Request) {
	svc, err := s.store.GetServiceByName(r.Context(), r.PathValue("name"))
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	revisions, err := s.store.ListRevisions(r.Context(), svc.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revisions})
}
