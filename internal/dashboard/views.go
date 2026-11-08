package dashboard

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The types below are the parts of the API's JSON the pages show. They are the
// dashboard's reading of the API, not the store's types: if the API changes a
// field the dashboard needs, the pages' tests fail.

type apiResources struct {
	CPUMillis   int64 `json:"cpu_millis"`
	MemoryBytes int64 `json:"memory_bytes"`
}

type apiNode struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	CPUMillis      int64             `json:"cpu_millis"`
	MemoryBytes    int64             `json:"memory_bytes"`
	Labels         map[string]string `json:"labels"`
	AgentVersion   string            `json:"agent_version"`
	RegisteredAt   time.Time         `json:"registered_at"`
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
	Status         string            `json:"status"`
	StatusReason   string            `json:"status_reason"`
	Health         string            `json:"health"`
	HealthReason   string            `json:"health_reason"`
	HealthSince    *time.Time        `json:"health_since"`
	LeaseExpiresAt *time.Time        `json:"lease_expires_at"`
	Allocatable    apiResources      `json:"allocatable"`
	Allocated      struct {
		apiResources
		Workloads int `json:"workloads"`
	} `json:"allocated"`
}

type apiReplica struct {
	Revision     int    `json:"revision"`
	Index        int    `json:"index"`
	WorkloadID   string `json:"workload_id"`
	WorkloadName string `json:"workload_name"`
	NodeID       string `json:"node_id"`
	DesiredState string `json:"desired_state"`
	Generation   int64  `json:"generation"`
	Acknowledged bool   `json:"acknowledged"`
	Phase        string `json:"phase"`
	Message      string `json:"message"`
	ContainerID  string `json:"container_id"`
	Health       string `json:"health"`
	RestartCount int    `json:"restart_count"`
	ExitCode     *int   `json:"exit_code"`
	NodeHealth   string `json:"node_health"`
}

type apiUnschedulable struct {
	Revision     int       `json:"revision"`
	ReplicaIndex int       `json:"replica_index"`
	Code         string    `json:"code"`
	Message      string    `json:"message"`
	Since        time.Time `json:"since"`
	Nodes        []struct {
		NodeID  string `json:"node_id"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"nodes"`
}

type apiRollout struct {
	Revision    int        `json:"revision"`
	State       string     `json:"state"`
	Reason      string     `json:"reason"`
	Message     string     `json:"message"`
	Paused      bool       `json:"paused"`
	Desired     int        `json:"desired"`
	Updated     int        `json:"updated"`
	Ready       int        `json:"ready"`
	Outdated    int        `json:"outdated"`
	Surge       int        `json:"surge"`
	Unavailable int        `json:"unavailable"`
	StartedAt   *time.Time `json:"started_at"`
	ProgressAt  *time.Time `json:"progress_at"`
	DeadlineAt  *time.Time `json:"deadline_at"`
	FinishedAt  *time.Time `json:"finished_at"`
}

type apiScaling struct {
	State    string `json:"state"`
	Desired  int    `json:"desired"`
	Ready    int    `json:"ready"`
	Starting int    `json:"starting"`
	Waiting  int    `json:"waiting"`
	Surplus  int    `json:"surplus"`
	Stopping int    `json:"stopping"`
	Outdated int    `json:"outdated"`
	Message  string `json:"message"`
}

type apiStatus struct {
	DesiredReplicas       int                `json:"desired_replicas"`
	ScheduledReplicas     int                `json:"scheduled_replicas"`
	UnschedulableReplicas int                `json:"unschedulable_replicas"`
	RunningReplicas       int                `json:"running_replicas"`
	FailedReplicas        int                `json:"failed_replicas"`
	UnhealthyReplicas     int                `json:"unhealthy_replicas"`
	LostReplicas          int                `json:"lost_replicas"`
	Scaling               apiScaling         `json:"scaling"`
	Rollout               apiRollout         `json:"rollout"`
	Replicas              []apiReplica       `json:"replicas"`
	Unschedulable         []apiUnschedulable `json:"unschedulable"`
}

type apiService struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Replicas int    `json:"replicas"`
	Revision int    `json:"revision"`
	Spec     struct {
		Image       string `json:"image"`
		CPUMillis   int64  `json:"cpu_millis"`
		MemoryBytes int64  `json:"memory_bytes"`
	} `json:"spec"`
	Version   int64     `json:"version"`
	Status    apiStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type apiObservation struct {
	State        string    `json:"state"`
	Message      string    `json:"message"`
	Health       string    `json:"health"`
	RestartCount int       `json:"restart_count"`
	ObservedAt   time.Time `json:"observed_at"`
}

type apiWorkload struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Image           string          `json:"image"`
	CPUMillis       int64           `json:"cpu_millis"`
	MemoryBytes     int64           `json:"memory_bytes"`
	NodeID          string          `json:"node_id"`
	DesiredState    string          `json:"desired_state"`
	ServiceID       string          `json:"service_id"`
	ServiceRevision int             `json:"service_revision"`
	ReplicaIndex    int             `json:"replica_index"`
	Observed        *apiObservation `json:"observed"`
}

type apiEvent struct {
	ID           int64             `json:"id"`
	At           time.Time         `json:"at"`
	InstanceID   string            `json:"instance_id"`
	Actor        string            `json:"actor"`
	Action       string            `json:"action"`
	ResourceType string            `json:"resource_type"`
	ResourceID   string            `json:"resource_id"`
	Message      string            `json:"message"`
	Details      map[string]string `json:"details"`
}

// base is what every page's layout needs.
type base struct {
	Title  string
	Active string // the navigation entry to highlight; empty on pages that are not behind the login
	// Refresh is the seconds until the page reloads itself, 0 for never, and
	// RefreshMeta the tag that makes it so. A page with a form never reloads: it
	// would lose what was typed.
	Refresh     int
	RefreshMeta template.HTML
	Generated   time.Time
}

func (d *Dashboard) head(title, active string) base {
	b := base{Title: title, Active: active, Generated: d.cfg.Now().UTC()}
	if d.cfg.Refresh > 0 && active != "" {
		b.Refresh = max(1, int(d.cfg.Refresh/time.Second))
		// An integer is all that goes into the tag, so it is safe as HTML.
		b.RefreshMeta = template.HTML(fmt.Sprintf(`<meta http-equiv="refresh" content="%d">`, b.Refresh))
	}
	return b
}

// summary is the cluster at a glance.
type summary struct {
	NodesHealthy, NodesSuspect, NodesUnavailable int

	CPUAllocatable, CPUAllocated int64
	MemAllocatable, MemAllocated int64

	Services        int
	ReplicasDesired int
	ReplicasRunning int
	// ReplicasProblem are the replicas that failed, are unhealthy, sit on an
	// unavailable node or have no node that can take them.
	ReplicasProblem int
	RolloutsFailed  int
}

func summarize(nodes []apiNode, services []apiService) summary {
	var s summary
	for _, n := range nodes {
		switch n.Health {
		case "healthy":
			s.NodesHealthy++
		case "suspect":
			s.NodesSuspect++
		case "unavailable":
			s.NodesUnavailable++
		}
		s.CPUAllocatable += n.Allocatable.CPUMillis
		s.CPUAllocated += n.Allocated.CPUMillis
		s.MemAllocatable += n.Allocatable.MemoryBytes
		s.MemAllocated += n.Allocated.MemoryBytes
	}
	s.Services = len(services)
	for _, svc := range services {
		st := svc.Status
		s.ReplicasDesired += svc.Replicas
		s.ReplicasRunning += st.Scaling.Ready
		s.ReplicasProblem += st.FailedReplicas + st.UnhealthyReplicas + st.LostReplicas + st.UnschedulableReplicas
		if st.Rollout.State == "failed" {
			s.RolloutsFailed++
		}
	}
	return s
}

// severity orders nodes so that those that need attention come first.
func severity(health string) int {
	switch health {
	case "unavailable":
		return 0
	case "suspect":
		return 1
	}
	return 2
}

func sortNodes(nodes []apiNode) {
	sort.SliceStable(nodes, func(i, j int) bool { return severity(nodes[i].Health) < severity(nodes[j].Health) })
}

type overviewPage struct {
	base
	Summary  summary
	Nodes    []apiNode
	Services []apiService
	Events   []apiEvent
}

func (d *Dashboard) overview(w http.ResponseWriter, r *http.Request) {
	var nodes struct {
		Nodes []apiNode `json:"nodes"`
	}
	var services struct {
		Services []apiService `json:"services"`
	}
	var events struct {
		Events []apiEvent `json:"events"`
	}
	for _, call := range []struct {
		path string
		out  any
	}{{"/v1/nodes", &nodes}, {"/v1/services", &services}, {"/v1/events?limit=15", &events}} {
		if err := d.get(r, call.path, call.out); err != nil {
			d.fail(w, r, err)
			return
		}
	}
	sortNodes(nodes.Nodes)
	d.render(w, http.StatusOK, "overview", overviewPage{
		base: d.head("Overview", "overview"), Summary: summarize(nodes.Nodes, services.Services),
		Nodes: nodes.Nodes, Services: services.Services, Events: events.Events,
	})
}

type servicePage struct {
	base
	Service apiService
	Events  []apiEvent
}

func (d *Dashboard) service(w http.ResponseWriter, r *http.Request) {
	var svc apiService
	if err := d.get(r, "/v1/services/"+url.PathEscape(r.PathValue("name")), &svc); err != nil {
		d.fail(w, r, err)
		return
	}
	var events struct {
		Events []apiEvent `json:"events"`
	}
	query := url.Values{"resource_type": {"service"}, "resource_id": {svc.ID}, "limit": {"20"}}
	if err := d.get(r, "/v1/events?"+query.Encode(), &events); err != nil {
		d.fail(w, r, err)
		return
	}
	d.render(w, http.StatusOK, "service", servicePage{base: d.head("Service "+svc.Name, "overview"), Service: svc, Events: events.Events})
}

type nodePage struct {
	base
	Node         apiNode
	Workloads    []apiWorkload
	ServiceNames map[string]string
	Events       []apiEvent
}

func (d *Dashboard) node(w http.ResponseWriter, r *http.Request) {
	var n apiNode
	if err := d.get(r, "/v1/nodes/"+url.PathEscape(r.PathValue("id")), &n); err != nil {
		d.fail(w, r, err)
		return
	}
	var workloads struct {
		Workloads []apiWorkload `json:"workloads"`
	}
	var services struct {
		Services []apiService `json:"services"`
	}
	var events struct {
		Events []apiEvent `json:"events"`
	}
	query := url.Values{"resource_type": {"node"}, "resource_id": {n.ID}, "limit": {"20"}}
	for _, call := range []struct {
		path string
		out  any
	}{{"/v1/workloads", &workloads}, {"/v1/services", &services}, {"/v1/events?" + query.Encode(), &events}} {
		if err := d.get(r, call.path, call.out); err != nil {
			d.fail(w, r, err)
			return
		}
	}
	page := nodePage{base: d.head("Node "+n.ID, "overview"), Node: n, ServiceNames: map[string]string{}, Events: events.Events}
	for _, svc := range services.Services {
		page.ServiceNames[svc.ID] = svc.Name
	}
	for _, wl := range workloads.Workloads {
		if wl.NodeID == n.ID {
			page.Workloads = append(page.Workloads, wl)
		}
	}
	d.render(w, http.StatusOK, "node", page)
}

// eventsPerPage is how many events the events page shows at once.
const eventsPerPage = 50

var (
	resourceTypePattern = regexp.MustCompile(`^[a-z_]{1,32}$`)
	resourceIDPattern   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

type eventsPage struct {
	base
	Events       []apiEvent
	ResourceType string
	ResourceID   string
	NextURL      string
}

func (d *Dashboard) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := url.Values{"limit": {strconv.Itoa(eventsPerPage)}}
	page := eventsPage{base: d.head("Events", "events"), ResourceType: q.Get("resource_type"), ResourceID: q.Get("resource_id")}
	invalid := func(what string) {
		d.render(w, http.StatusBadRequest, "error", errorPage{base: d.head("Error", "overview"), Status: http.StatusBadRequest, Message: "Invalid " + what + "."})
	}
	if page.ResourceType != "" {
		if !resourceTypePattern.MatchString(page.ResourceType) {
			invalid("resource type")
			return
		}
		query.Set("resource_type", page.ResourceType)
	}
	if page.ResourceID != "" {
		if !resourceIDPattern.MatchString(page.ResourceID) {
			invalid("resource ID")
			return
		}
		query.Set("resource_id", page.ResourceID)
	}
	if before := q.Get("before"); before != "" {
		if n, err := strconv.ParseInt(before, 10, 64); err != nil || n < 1 {
			invalid("page")
			return
		}
		query.Set("before", before)
	}
	var events struct {
		Events []apiEvent `json:"events"`
	}
	if err := d.get(r, "/v1/events?"+query.Encode(), &events); err != nil {
		d.fail(w, r, err)
		return
	}
	page.Events = events.Events
	if len(events.Events) == eventsPerPage {
		next := url.Values{"before": {strconv.FormatInt(events.Events[len(events.Events)-1].ID, 10)}}
		for _, k := range []string{"resource_type", "resource_id"} {
			if v := query.Get(k); v != "" {
				next.Set(k, v)
			}
		}
		page.NextURL = "/dashboard/events?" + next.Encode()
	}
	d.render(w, http.StatusOK, "events", page)
}

func (d *Dashboard) parsePages() (map[string]*template.Template, error) {
	pages := map[string]*template.Template{}
	for _, name := range []string{"login", "overview", "service", "node", "events", "error"} {
		t, err := template.New(name).Funcs(d.funcs()).ParseFS(assets, "templates/layout.html", "templates/"+name+".html")
		if err != nil {
			return nil, fmt.Errorf("dashboard: parse the %s page: %w", name, err)
		}
		pages[name] = t
	}
	return pages, nil
}

func (d *Dashboard) funcs() template.FuncMap {
	return template.FuncMap{
		"ago":   d.ago,
		"stamp": stamp,
		"mem":   formatBytes,
		"cpu":   formatCPU,
		"badge": badgeClass,
		"kv":    keyValues,
		"deref": deref,
	}
}

// deref reads through the optional values the API sends (a missing one is the zero value).
func deref(v any) any {
	switch p := v.(type) {
	case *time.Time:
		if p == nil {
			return time.Time{}
		}
		return *p
	case *int:
		if p == nil {
			return 0
		}
		return *p
	}
	return v
}

// ago says how long before the page was made a time was: "12s ago". A zero time is "never".
func (d *Dashboard) ago(t time.Time) string {
	if t.IsZero() || t.Year() <= 1 {
		return "never"
	}
	age := d.cfg.Now().Sub(t)
	switch {
	case age < 5*time.Second:
		return "just now"
	case age < time.Minute:
		return fmt.Sprintf("%ds ago", int(age/time.Second))
	case age < time.Hour:
		return fmt.Sprintf("%dm ago", int(age/time.Minute))
	case age < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(age/time.Hour))
	}
	return fmt.Sprintf("%dd ago", int(age/(24*time.Hour)))
}

func stamp(t time.Time) string {
	if t.IsZero() || t.Year() <= 1 {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, suffix := float64(n), "B"
	for _, s := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= unit
		suffix = s
		if value < unit {
			break
		}
	}
	return strings.TrimSuffix(strconv.FormatFloat(value, 'f', 1, 64), ".0") + " " + suffix
}

func formatCPU(millis int64) string {
	return strconv.FormatFloat(float64(millis)/1000, 'f', -1, 64) + " CPU"
}

// badgeClass picks the colour of a state: ok, warn or bad.
func badgeClass(state string) string {
	switch state {
	case "healthy", "ready", "running", "complete", "succeeded", "stable":
		return "ok"
	case "suspect", "starting", "pending", "scheduled", "progressing", "paused", "scaling_up", "scaling_down", "updating", "stopping", "not_ready":
		return "warn"
	case "unavailable", "failed", "unhealthy", "lost", "blocked", "degraded", "error", "unschedulable":
		return "bad"
	}
	return ""
}

// keyValues lists a map as sorted "key=value" strings.
func keyValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
