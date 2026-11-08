package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Scaling changes how many replicas a service wants and nothing else. Like the
// rest of the services API it only records intent: the scheduler creates the
// replicas that are missing, the reconciler stops the ones in surplus, and the
// agents start and stop the containers. What a request returns is the progress
// at that moment, and the same view is there to be read until it is done.

var scaleFields = fieldSet("replicas")

// scaleView is a service's replica count and how close the cluster is to it.
type scaleView struct {
	Name     string `json:"name"`
	Revision int    `json:"revision"`
	Version  int64  `json:"version"`

	// Replicas is the wanted count. PreviousReplicas and Changed are only in the
	// answer to a PUT: what the count was, and whether the request changed it
	// (a request for the count the service already has changes nothing).
	Replicas         int   `json:"replicas"`
	PreviousReplicas *int  `json:"previous_replicas,omitempty"`
	Changed          *bool `json:"changed,omitempty"`

	Scaling scalingStatus `json:"scaling"`
}

func writeScale(w http.ResponseWriter, v scaleView) {
	w.Header().Set("ETag", etag(v.Version))
	writeJSON(w, http.StatusOK, v)
}

// readScaleRequest reads and validates {"replicas": N}. It has answered the
// request already when ok is false.
func readScaleRequest(w http.ResponseWriter, r *http.Request) (replicas int, ok bool) {
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
	invalid := func(p problems) (int, bool) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid scale request", "details": p})
		return 0, false
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil || top == nil {
		return invalid(problems{{Message: `the request body must be a JSON object such as {"replicas": 3}`}})
	}
	var p problems
	p.refuseUnknown("", top, scaleFields)
	var b struct {
		Replicas *int `json:"replicas"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			p.add(typeErr.Field, "must be an integer")
		} else {
			p.add("", "the request body is not a valid scale request")
		}
	}
	switch {
	case len(p) > 0:
	case b.Replicas == nil:
		p.add("replicas", "is required")
	case *b.Replicas < 0 || *b.Replicas > maxReplicas:
		p.add("replicas", "must be between 0 and %d", maxReplicas)
	}
	if len(p) > 0 {
		return invalid(p)
	}
	return *b.Replicas, true
}

// getScale reports the wanted replica count and the progress towards it.
func (s *server) getScale(w http.ResponseWriter, r *http.Request) {
	svc, _, err := s.liveService(r, s.store)
	if err != nil {
		s.failService(w, r, err, false)
		return
	}
	status, err := s.statusOf(r.Context(), svc)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeScale(w, scaleView{
		Name: svc.Name, Revision: svc.CurrentRevision, Version: svc.Version, Replicas: svc.Replicas, Scaling: status.Scaling,
	})
}

// putScale sets the wanted replica count. It is idempotent: the request says
// what the count should be, not by how much to change it, so a request that
// asks for the count the service already has changes nothing, records nothing,
// and succeeds, even if its If-Match is out of date. A request that was applied
// but whose answer was lost can therefore be sent again. With If-Match, a
// request that does change the count only applies to the version the client read.
func (s *server) putScale(w http.ResponseWriter, r *http.Request) {
	want, ok := readIfMatch(w, r)
	if !ok {
		return
	}
	replicas, ok := readScaleRequest(w, r)
	if !ok {
		return
	}
	var out store.Service
	var previous int
	var changed bool
	err := s.store.InTx(r.Context(), func(tx store.Tx) error {
		ctx := r.Context()
		svc, _, err := s.liveService(r, tx)
		if err != nil {
			return err
		}
		out, previous, changed = svc, svc.Replicas, false
		if replicas == svc.Replicas {
			return nil
		}
		if want != 0 && svc.Version != want {
			return store.ErrVersionConflict
		}
		scaled, err := tx.ScaleService(ctx, svc.ID, replicas, svc.Version, s.now().UTC())
		switch {
		case errors.Is(err, store.ErrUnchanged):
			out = scaled
			return nil
		case err != nil:
			return err
		}
		out, changed = scaled, true
		_, err = tx.AppendEvent(ctx, s.event("service.scaled", "service", svc.ID, "", map[string]string{
			"name": svc.Name, "from_replicas": strconv.Itoa(previous), "to_replicas": strconv.Itoa(replicas),
		}))
		return err
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
	writeScale(w, scaleView{
		Name: out.Name, Revision: out.CurrentRevision, Version: out.Version, Replicas: out.Replicas,
		PreviousReplicas: &previous, Changed: &changed, Scaling: status.Scaling,
	})
}
