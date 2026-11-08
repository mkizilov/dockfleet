package api

import (
	"context"
	"net/http"
	"time"

	"github.com/dockfleet/dockfleet/internal/election"
	"github.com/dockfleet/dockfleet/internal/metrics"
)

// Cluster is the election as the API sees it. *election.Elector implements it.
type Cluster interface {
	election.Fence
	Status() election.Status
}

// Modes of GET /v1/cluster.
const (
	modeSingle = "single"
	modeHA     = "high-availability"
	roleSingle = "single"
)

// controlPlaneView is the fencing record in the shared database: the epoch
// every manager's work is checked against.
type controlPlaneView struct {
	Epoch     int64      `json:"epoch"`
	Leader    string     `json:"leader,omitempty"`
	ElectedAt *time.Time `json:"elected_at,omitempty"`
}

// clusterView says how this manager sits in the cluster of managers. It answers
// on every manager, the leader or not, so that an operator can ask any of them
// who leads.
type clusterView struct {
	// Mode is "single" for a manager that runs without leader election, or
	// "high-availability".
	Mode       string `json:"mode"`
	InstanceID string `json:"instance_id"`
	// Role is "leader" or "follower"; "single" without leader election.
	Role string `json:"role"`
	// Leader is the leader as this manager knows it: itself while it leads,
	// otherwise the last holder it saw ("" if none or unknown).
	Leader string `json:"leader,omitempty"`
	// Epoch is the epoch this manager acts under while it leads, and 0 otherwise.
	Epoch int64 `json:"epoch"`
	// Since is when this manager took its role.
	Since *time.Time `json:"since,omitempty"`
	// ValidUntil is, for a leader, the time after which it stops acting unless
	// it renews its lease before.
	ValidUntil *time.Time `json:"valid_until,omitempty"`
	// ControlPlane is the record in the shared database: the latest epoch, whoever
	// began it. A leader's Epoch equals it unless the leader has been replaced
	// and does not know yet.
	ControlPlane controlPlaneView `json:"control_plane"`
	Transitions  uint64           `json:"transitions"`
	Errors       uint64           `json:"errors"`
	LastError    string           `json:"last_error,omitempty"`
}

func (s *server) getCluster(w http.ResponseWriter, r *http.Request) {
	cp, err := s.store.GetControlPlane(r.Context())
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	view := clusterView{Mode: modeSingle, InstanceID: s.instanceID, Role: roleSingle, ControlPlane: controlPlaneView{Epoch: cp.Epoch, Leader: cp.Leader}}
	if cp.Epoch > 0 {
		at := cp.ElectedAt.UTC()
		view.ControlPlane.ElectedAt = &at
	}
	if s.cluster != nil {
		st := s.cluster.Status()
		view.Mode, view.InstanceID, view.Role = modeHA, st.ID, string(st.Role)
		view.Leader, view.Epoch, view.Transitions, view.Errors, view.LastError = st.Leader, st.Epoch, st.Transitions, st.Errors, st.LastError
		since := st.Since.UTC()
		view.Since = &since
		if !st.ValidUntil.IsZero() {
			until := st.ValidUntil.UTC()
			view.ValidUntil = &until
		}
	}
	writeJSON(w, http.StatusOK, view)
}

// collectCluster reports this manager's part in the election and the epoch in
// the shared database. Every manager reports about itself, so a scrape of all
// of them shows who leads, and an epoch that differs from the database's shows a
// leader that has been replaced and does not know yet.
func (s *server) collectCluster(ctx context.Context) ([]metrics.Family, error) {
	st := s.cluster.Status()
	cp, err := s.store.GetControlPlane(ctx)
	if err != nil {
		return nil, err
	}
	leader := metrics.NewFamily("dockfleet_manager_is_leader", "1 if this manager is the leader of its cluster, 0 if it follows.", metrics.GaugeType)
	leader.Add(bool01(st.Role == election.Leader))
	epoch := metrics.NewFamily("dockfleet_manager_epoch", "The control-plane epoch this manager acts under while it leads, and 0 while it follows.", metrics.GaugeType)
	epoch.Add(float64(st.Epoch))
	current := metrics.NewFamily("dockfleet_control_plane_epoch", "The latest control-plane epoch in the shared database: it grows by one every time a manager is elected leader.", metrics.GaugeType)
	current.Add(float64(cp.Epoch))
	info := metrics.NewFamily("dockfleet_cluster_leader", "1 for the leader as this manager knows it (the label is empty while it knows none).", metrics.GaugeType)
	info.Add(1, "leader", st.Leader)
	transitions := metrics.NewFamily("dockfleet_leader_transitions_total", "Times this manager became the leader or stopped being it.", metrics.CounterType)
	transitions.Add(float64(st.Transitions))
	errs := metrics.NewFamily("dockfleet_election_errors_total", "Election steps that failed against etcd or the database.", metrics.CounterType)
	errs.Add(float64(st.Errors))
	return []metrics.Family{*leader, *epoch, *current, *info, *transitions, *errs}, nil
}

// leaderEpoch is the epoch a request that decides things for the cluster acts
// under: 0 without leader election. A manager that does not lead refuses the
// request itself, with the leader's name if it knows it, and returns false.
func (s *server) leaderEpoch(w http.ResponseWriter) (int64, bool) {
	if s.cluster == nil {
		return 0, true
	}
	epoch, err := s.cluster.Epoch()
	if err != nil {
		s.notLeader(w)
		return 0, false
	}
	return epoch, true
}

func (s *server) notLeader(w http.ResponseWriter) {
	body := map[string]string{
		"error": "this manager is not the leader of its cluster; send the request to the leader",
		"code":  "not_leader",
	}
	if leader := s.cluster.Status().Leader; leader != "" {
		body["leader"] = leader
	}
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, body)
}
