package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Rolling deployments. A change of spec is a new revision (PUT /v1/services/{name});
// how it replaces the running replicas is the service's rollout policy; pausing,
// resuming and rolling back are requests like any other here: they record intent
// and nothing more. The scheduler creates the replicas of the new spec as far as
// the surge allows, the reconciler retires old ones as far as the unavailable
// limit allows and keeps the record of the rollout, and the agents run the
// containers.

// rolloutRecord is one revision's rollout in the history.
type rolloutRecord struct {
	Revision   int        `json:"revision"`
	State      string     `json:"state"`
	Reason     string     `json:"reason,omitempty"`
	Message    string     `json:"message,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	ProgressAt time.Time  `json:"progress_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// rolloutView is a service's rollout policy, whether it is paused, how far the
// current deployment has come and how earlier ones ended.
type rolloutView struct {
	Name     string              `json:"name"`
	Revision int                 `json:"revision"`
	Version  int64               `json:"version"`
	Policy   store.RolloutPolicy `json:"policy"`
	Paused   bool                `json:"paused"`
	Rollout  rolloutStatus       `json:"rollout"`
	History  []rolloutRecord     `json:"history"`

	// Changed is in the answer to a POST only: whether the request changed
	// anything. RolledBackTo is the revision whose spec a rollback returns to;
	// Revision is the new revision that carries it.
	Changed      *bool `json:"changed,omitempty"`
	RolledBackTo int   `json:"rolled_back_to,omitempty"`
}

func (s *server) rolloutViewOf(ctx context.Context, svc store.Service) (rolloutView, error) {
	status, err := s.statusOf(ctx, svc)
	if err != nil {
		return rolloutView{}, err
	}
	records, err := s.store.ListRollouts(ctx, svc.ID)
	if err != nil {
		return rolloutView{}, err
	}
	v := rolloutView{
		Name: svc.Name, Revision: svc.CurrentRevision, Version: svc.Version, Policy: svc.Rollout, Paused: svc.Paused,
		Rollout: status.Rollout, History: make([]rolloutRecord, 0, len(records)),
	}
	for _, rec := range records {
		v.History = append(v.History, rolloutRecord{
			Revision: rec.Revision, State: rec.State, Reason: rec.Reason, Message: rec.Message,
			StartedAt: rec.StartedAt, ProgressAt: rec.ProgressAt, FinishedAt: rec.FinishedAt,
		})
	}
	return v, nil
}

func (s *server) writeRolloutView(w http.ResponseWriter, r *http.Request, svc store.Service, changed *bool, rolledBackTo int) {
	v, err := s.rolloutViewOf(r.Context(), svc)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	v.Changed, v.RolledBackTo = changed, rolledBackTo
	w.Header().Set("ETag", etag(v.Version))
	writeJSON(w, http.StatusOK, v)
}

// getRollout reports the rollout policy and the progress of the deployment.
func (s *server) getRollout(w http.ResponseWriter, r *http.Request) {
	svc, _, err := s.liveService(r, s.store)
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	s.writeRolloutView(w, r, svc, nil, 0)
}

func (s *server) pauseRollout(w http.ResponseWriter, r *http.Request)  { s.holdRollout(w, r, true) }
func (s *server) resumeRollout(w http.ResponseWriter, r *http.Request) { s.holdRollout(w, r, false) }

// holdRollout pauses or resumes the rollouts of a service. While it is paused no
// replica of a new spec is added and no old one is retired for it; replicas that
// fail or are lost are still replaced, a scale-down still happens, and the
// progress deadline stops running. Like scaling it is idempotent: asking for
// the state the service is in changes nothing, records nothing, and succeeds,
// even with an out-of-date If-Match, so a request whose answer was lost can be
// sent again.
func (s *server) holdRollout(w http.ResponseWriter, r *http.Request, paused bool) {
	want, ok := readIfMatch(w, r)
	if !ok {
		return
	}
	action := "service.resumed"
	if paused {
		action = "service.paused"
	}
	var out store.Service
	changed := false
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx := r.Context()
		svc, _, err := s.liveService(r, tx)
		if err != nil {
			return err
		}
		out, changed = svc, false
		if svc.Paused == paused {
			return nil
		}
		if want != 0 && svc.Version != want {
			return store.ErrVersionConflict
		}
		held, err := tx.SetRollout(ctx, svc.ID, svc.Rollout, paused, svc.Version, s.now().UTC())
		switch {
		case errors.Is(err, store.ErrUnchanged):
			out = held
			return nil
		case err != nil:
			return err
		}
		out, changed = held, true
		_, err = tx.AppendEvent(ctx, s.event(ctx, action, "service", svc.ID, "", map[string]string{
			"name": svc.Name, "revision": strconv.Itoa(svc.CurrentRevision),
		}))
		return err
	})
	if err != nil {
		s.failService(w, r, err, want != 0)
		return
	}
	s.writeRolloutView(w, r, out, &changed, 0)
}

var rollbackFields = fieldSet("revision")

// readRollbackRequest reads the optional body {"revision": N}. It returns 0
// for none: go back to the previous revision. It has answered the request
// already when ok is false.
func readRollbackRequest(w http.ResponseWriter, r *http.Request) (revision int, ok bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, "the request body is too large")
		return 0, false
	case err != nil:
		writeError(w, http.StatusBadRequest, "the request body could not be read")
		return 0, false
	}
	if strings.TrimSpace(string(body)) == "" {
		return 0, true
	}
	invalid := func(p problems) (int, bool) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid rollback request", "details": p})
		return 0, false
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil || top == nil {
		return invalid(problems{{Message: `the request body must be empty or a JSON object such as {"revision": 2}`}})
	}
	var p problems
	p.refuseUnknown("", top, rollbackFields)
	var b struct {
		Revision *int `json:"revision"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			p.add(typeErr.Field, "must be an integer")
		} else {
			p.add("", "the request body is not a valid rollback request")
		}
	}
	switch {
	case len(p) > 0:
	case b.Revision == nil:
		p.add("revision", "is required when a body is sent")
	case *b.Revision < 1:
		p.add("revision", "must be at least 1")
	}
	if len(p) > 0 {
		return invalid(p)
	}
	return *b.Revision, true
}

// rollbackService returns a service to an earlier spec by deploying it again as
// a new revision, so the history stays linear and every revision is a faithful
// record of what was once wanted. Without a body the target is the latest
// earlier revision whose spec differs from the current one; with {"revision": N}
// it is revision N. Replicas that still run the target's spec are kept, so
// returning from a rollout that has gone wrong does not restart the replicas it
// had not reached. A rollback clears a pause: it is the way out of a bad
// rollout, and it goes ahead.
//
// Naming the revision makes the request idempotent: if the service already runs
// that spec nothing changes and the answer says so. "The previous revision" is
// relative to where the service is now, so repeating that request would go back
// to the revision it came from; send If-Match to make it safe to repeat.
func (s *server) rollbackService(w http.ResponseWriter, r *http.Request) {
	want, ok := readIfMatch(w, r)
	if !ok {
		return
	}
	target, ok := readRollbackRequest(w, r)
	if !ok {
		return
	}
	var out store.Service
	var to store.Revision
	changed := false
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx := r.Context()
		svc, cur, err := s.liveService(r, tx)
		if err != nil {
			return err
		}
		revs, err := tx.ListRevisions(ctx, svc.ID)
		if err != nil {
			return err
		}
		if target > 0 {
			if target > len(revs) {
				return requestError{status: http.StatusNotFound, message: fmt.Sprintf("revision %d does not exist", target)}
			}
			to = revs[target-1]
		} else {
			found := false
			for i := len(revs) - 1; i >= 0; i-- {
				if revs[i].Revision < cur.Revision && revs[i].SpecHash != cur.SpecHash {
					to, found = revs[i], true
					break
				}
			}
			if !found {
				return requestError{status: http.StatusConflict, message: "there is no earlier revision with a different spec to roll back to"}
			}
		}
		out, changed = svc, false
		if to.SpecHash == cur.SpecHash {
			return nil // the service already runs that spec
		}
		if want != 0 && svc.Version != want {
			return store.ErrVersionConflict
		}
		now := s.now().UTC()
		deployed, rev, err := tx.DeployService(ctx, svc.ID, store.Deployment{
			Spec: to.Spec, Message: fmt.Sprintf("rollback to revision %d", to.Revision), CreatedBy: adminActor,
			ExpectedVersion: svc.Version, At: now,
		})
		switch {
		case errors.Is(err, store.ErrUnchanged):
			return nil
		case err != nil:
			return err
		}
		out, changed = deployed, true
		if deployed.Paused {
			if out, err = tx.SetRollout(ctx, deployed.ID, deployed.Rollout, false, deployed.Version, now); err != nil {
				return err
			}
		}
		_, err = tx.AppendEvent(ctx, s.event(ctx, "service.rolled_back", "service", svc.ID, "", map[string]string{
			"name": svc.Name, "from_revision": strconv.Itoa(cur.Revision), "to_revision": strconv.Itoa(rev.Revision),
			"target_revision": strconv.Itoa(to.Revision), "image": to.Spec.Image,
		}))
		return err
	})
	if err != nil {
		s.failService(w, r, err, want != 0)
		return
	}
	s.writeRolloutView(w, r, out, &changed, to.Revision)
}
