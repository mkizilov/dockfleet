package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/dockfleet/dockfleet/internal/store"
)

// Limits on service definitions.
//
// The agent re-checks every container it creates against its own policy
// (internal/docker/policy.go) and the manager must not import that package,
// so these limits are kept at least as strict as the agent's: a definition
// accepted here must not be one an agent would refuse to run.
const (
	maxReplicas         = 1000
	maxServiceCPUMillis = 1_000_000 // 1000 CPUs; 1000 millis is one CPU
	minServiceMemory    = 6 << 20
	maxServiceMemory    = 1 << 40
	maxCommandArgs      = 128
	maxCommandArgBytes  = 4096
	maxEnvEntries       = 128
	maxEnvNameLength    = 256
	maxEnvValueBytes    = 32 << 10
	maxServicePorts     = 32
	minHostPort         = 1024 // lower ports are privileged
	maxHealthArgs       = 32
	maxHealthArgBytes   = 1024
	maxSelectorEntries  = 16
	maxSelectorLength   = 128
	maxRestartRetries   = 100

	defaultMaxRetries        = 5
	defaultBackoffSeconds    = 1
	defaultMaxBackoffSeconds = 60
	maxBackoffSeconds        = 3600
	defaultHealthInterval    = 30
	defaultHealthTimeout     = 5
	defaultHealthRetries     = 3
	maxHealthIntervalSeconds = 3600
	maxHealthTimeoutSeconds  = 300
	maxHealthStartPeriod     = 3600
	maxHealthRetries         = 10
)

// problem is one thing wrong with a request, tied to the field that caused it.
type problem struct {
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

type problems []problem

func (p *problems) add(field, format string, args ...any) {
	*p = append(*p, problem{Field: field, Message: fmt.Sprintf(format, args...)})
}

// The request shapes use pointers so a missing field can be told from a zero one.
type serviceBody struct {
	Name     *string   `json:"name"`
	Replicas *int      `json:"replicas"`
	Spec     *specBody `json:"spec"`
}

type specBody struct {
	Image         *string           `json:"image"`
	Command       []string          `json:"command"`
	Env           map[string]string `json:"env"`
	CPUMillis     *int64            `json:"cpu_millis"`
	MemoryBytes   *int64            `json:"memory_bytes"`
	Ports         []portBody        `json:"ports"`
	HealthCheck   *healthBody       `json:"health_check"`
	RestartPolicy *restartBody      `json:"restart_policy"`
	NodeSelector  map[string]string `json:"node_selector"`
}

type portBody struct {
	ContainerPort *int    `json:"container_port"`
	Protocol      *string `json:"protocol"`
	HostPort      *int    `json:"host_port"`
}

type healthBody struct {
	Command            []string `json:"command"`
	IntervalSeconds    int      `json:"interval_seconds"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	StartPeriodSeconds int      `json:"start_period_seconds"`
	Retries            int      `json:"retries"`
}

type restartBody struct {
	Condition         string `json:"condition"`
	MaxRetries        int    `json:"max_retries"`
	BackoffSeconds    int    `json:"backoff_seconds"`
	MaxBackoffSeconds int    `json:"max_backoff_seconds"`
}

// serviceInput is a validated, normalized service definition.
type serviceInput struct {
	name     string
	replicas int
	spec     store.ServiceSpec
}

func fieldSet(names ...string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

// Only these fields are accepted. Everything else is refused rather than
// ignored, so a typo or a Docker option DockFleet does not implement can never
// be silently dropped.
var (
	topFields     = fieldSet("name", "replicas", "spec")
	specFields    = fieldSet("image", "command", "env", "cpu_millis", "memory_bytes", "ports", "health_check", "restart_policy", "node_selector")
	portFields    = fieldSet("container_port", "protocol", "host_port")
	healthFields  = fieldSet("command", "interval_seconds", "timeout_seconds", "start_period_seconds", "retries")
	restartFields = fieldSet("condition", "max_retries", "backoff_seconds", "max_backoff_seconds")

	// unsafeFields are Docker options that weaken the isolation between a
	// container and its host. They get a specific refusal so an administrator
	// learns it is a policy, not a typo.
	unsafeFields = fieldSet(
		"privileged", "cap_add", "devices", "device_cgroup_rules",
		"volumes", "volumes_from", "mounts", "binds",
		"network_mode", "pid", "pid_mode", "ipc", "ipc_mode", "uts", "uts_mode", "userns_mode",
		"security_opt", "sysctls", "cgroup_parent", "group_add", "host_ip", "extra_hosts",
	)

	readOnlyFields = fieldSet("id", "version", "revision", "status", "created_at", "updated_at", "deleted_at")
)

func fieldReason(key string) string {
	switch {
	case unsafeFields[key]:
		return "is not allowed: DockFleet never lets a service weaken the isolation between its container and the host"
	case readOnlyFields[key]:
		return "is read-only"
	}
	return "is not a supported field"
}

func joinField(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// object parses raw as a JSON object. A value of another type is left for the
// typed decode to report.
func object(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, false
	}
	return m, true
}

func (p *problems) refuseUnknown(path string, obj map[string]json.RawMessage, allowed map[string]bool) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !allowed[k] {
			p.add(joinField(path, k), "%s", fieldReason(k))
		}
	}
}

// checkFields walks the request and reports every field that is not on the
// allow-list, at any depth.
func (p *problems) checkFields(top map[string]json.RawMessage) {
	p.refuseUnknown("", top, topFields)
	spec, ok := object(top["spec"])
	if !ok {
		return
	}
	p.refuseUnknown("spec", spec, specFields)
	if obj, ok := object(spec["health_check"]); ok {
		p.refuseUnknown("spec.health_check", obj, healthFields)
	}
	if obj, ok := object(spec["restart_policy"]); ok {
		p.refuseUnknown("spec.restart_policy", obj, restartFields)
	}
	var ports []json.RawMessage
	if json.Unmarshal(spec["ports"], &ports) == nil {
		for i, item := range ports {
			if obj, ok := object(item); ok {
				p.refuseUnknown(fmt.Sprintf("spec.ports[%d]", i), obj, portFields)
			}
		}
	}
}

func jsonKind(t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Int, reflect.Int32, reflect.Int64:
		return "an integer"
	case reflect.Bool:
		return "a boolean"
	case reflect.Slice, reflect.Array:
		return "an array"
	case reflect.Map, reflect.Struct:
		return "an object"
	}
	return "of a different type"
}

// parseService decodes and validates a request body. creating is true for POST,
// where the name is required; for PUT the name may be left out, and if given it
// must be pathName. Every problem found is reported, not only the first.
func parseService(body []byte, creating bool, pathName string) (serviceInput, problems) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil || top == nil {
		return serviceInput{}, problems{{Message: "the request body must be a single JSON object"}}
	}
	var p problems
	p.checkFields(top)
	var b serviceBody
	if err := json.Unmarshal(body, &b); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			p.add(typeErr.Field, "must be %s", jsonKind(typeErr.Type))
		} else {
			p.add("", "the request body is not a valid service definition")
		}
	}
	if len(p) > 0 {
		return serviceInput{}, p
	}

	var in serviceInput
	switch {
	case creating && b.Name == nil:
		p.add("name", "is required")
	case creating && !store.ValidName(*b.Name):
		p.add("name", "must be 1-63 characters of [A-Za-z0-9._-] starting with a letter or digit")
	case !creating && b.Name != nil && *b.Name != pathName:
		p.add("name", "cannot be changed (this service is %q)", pathName)
	}
	if creating && b.Name != nil {
		in.name = *b.Name
	}
	switch {
	case b.Replicas == nil:
		p.add("replicas", "is required")
	case *b.Replicas < 0 || *b.Replicas > maxReplicas:
		p.add("replicas", "must be between 0 and %d", maxReplicas)
	default:
		in.replicas = *b.Replicas
	}
	if b.Spec == nil {
		p.add("spec", "is required")
	} else {
		in.spec = b.Spec.validate(&p)
	}
	if len(p) > 0 {
		return serviceInput{}, p
	}
	return in, nil
}

var (
	imageHost = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)*(?::[0-9]{1,5})?$`)
	imagePart = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
	imageTag  = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	imageHash = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

// validImage reports whether ref is a Docker image reference,
// [host[:port]/]repository[:tag][@sha256:digest], split the way Docker splits
// it: the first component is a registry host only if it contains a dot or a
// colon or is "localhost", and repository names are lower case.
func validImage(ref string) bool {
	if len(ref) > maxImageLength {
		return false
	}
	name := ref
	if before, digest, found := strings.Cut(name, "@"); found {
		if !imageHash.MatchString(digest) {
			return false
		}
		name = before
	}
	// A colon after the last slash starts the tag (before it, it is a registry port).
	if i := strings.LastIndex(name, ":"); i > strings.LastIndex(name, "/") {
		if !imageTag.MatchString(name[i+1:]) {
			return false
		}
		name = name[:i]
	}
	parts := strings.Split(name, "/")
	if len(parts) > 1 && (strings.ContainsAny(parts[0], ".:") || parts[0] == "localhost") {
		if !imageHost.MatchString(parts[0]) {
			return false
		}
		parts = parts[1:]
	}
	for _, part := range parts {
		if !imagePart.MatchString(part) {
			return false
		}
	}
	return true
}

var (
	envName     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	selectorKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)
)

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s specBody) validate(p *problems) store.ServiceSpec {
	var out store.ServiceSpec
	switch {
	case s.Image == nil || *s.Image == "":
		p.add("spec.image", "is required")
	case !validImage(*s.Image):
		p.add("spec.image", "must be a Docker image reference such as nginx:1.27 or registry.example.com/team/app@sha256:<digest> (at most %d characters, lower-case repository)", maxImageLength)
	default:
		out.Image = *s.Image
	}
	if validArgs(p, "spec.command", s.Command, maxCommandArgs, maxCommandArgBytes) {
		out.Command = append([]string(nil), s.Command...)
	}
	out.Env = validateEnv(p, s.Env)

	switch {
	case s.CPUMillis == nil:
		p.add("spec.cpu_millis", "is required: every service has a CPU limit")
	case *s.CPUMillis < 1 || *s.CPUMillis > maxServiceCPUMillis:
		p.add("spec.cpu_millis", "must be between 1 and %d (1000 is one CPU)", maxServiceCPUMillis)
	default:
		out.CPUMillis = *s.CPUMillis
	}
	switch {
	case s.MemoryBytes == nil:
		p.add("spec.memory_bytes", "is required: every service has a memory limit")
	case *s.MemoryBytes < minServiceMemory || *s.MemoryBytes > maxServiceMemory:
		p.add("spec.memory_bytes", "must be between %d (6 MiB) and %d bytes", minServiceMemory, int64(maxServiceMemory))
	default:
		out.MemoryBytes = *s.MemoryBytes
	}

	out.Ports = validatePorts(p, s.Ports)
	out.HealthCheck = validateHealth(p, s.HealthCheck)
	out.RestartPolicy = validateRestart(p, s.RestartPolicy)
	out.NodeSelector = validateSelector(p, s.NodeSelector)
	return out
}

// validArgs checks an argument vector (a command). It returns whether it is usable.
func validArgs(p *problems, field string, args []string, maxArgs, maxBytes int) bool {
	if len(args) == 0 {
		return false
	}
	ok := true
	if len(args) > maxArgs {
		p.add(field, "at most %d arguments are allowed", maxArgs)
		return false
	}
	if args[0] == "" {
		p.add(field, "the first element must be the program to run")
		ok = false
	}
	for i, arg := range args {
		if len(arg) > maxBytes || strings.Contains(arg, "\x00") {
			p.add(fmt.Sprintf("%s[%d]", field, i), "must be at most %d bytes and contain no NUL character", maxBytes)
			ok = false
		}
	}
	return ok
}

func validateEnv(p *problems, env map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	if len(env) > maxEnvEntries {
		p.add("spec.env", "at most %d variables are allowed", maxEnvEntries)
		return nil
	}
	ok := true
	for _, k := range sortedKeys(env) {
		field := fmt.Sprintf("spec.env[%q]", k)
		if len(k) > maxEnvNameLength || !envName.MatchString(k) {
			p.add(field, "the name must be at most %d characters of [A-Za-z0-9_], not starting with a digit", maxEnvNameLength)
			ok = false
		}
		// The value is never repeated back: it may be a secret.
		if v := env[k]; len(v) > maxEnvValueBytes || strings.Contains(v, "\x00") {
			p.add(field, "the value must be at most %d bytes and contain no NUL character", maxEnvValueBytes)
			ok = false
		}
	}
	if !ok {
		return nil
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		out[k] = v
	}
	return out
}

func validatePorts(p *problems, ports []portBody) []store.ServicePort {
	if len(ports) == 0 {
		return nil
	}
	if len(ports) > maxServicePorts {
		p.add("spec.ports", "at most %d ports are allowed", maxServicePorts)
		return nil
	}
	type slot struct {
		port  int
		proto string
	}
	containerSeen, hostSeen := map[slot]bool{}, map[slot]bool{}
	var out []store.ServicePort
	for i, in := range ports {
		field := fmt.Sprintf("spec.ports[%d]", i)
		port := store.ServicePort{Protocol: "tcp"}
		switch {
		case in.ContainerPort == nil:
			p.add(field+".container_port", "is required")
		case *in.ContainerPort < 1 || *in.ContainerPort > 65535:
			p.add(field+".container_port", "must be between 1 and 65535")
		default:
			port.ContainerPort = *in.ContainerPort
		}
		if in.Protocol != nil {
			switch *in.Protocol {
			case "tcp", "udp":
				port.Protocol = *in.Protocol
			default:
				p.add(field+".protocol", `must be "tcp" or "udp"`)
			}
		}
		if in.HostPort != nil && *in.HostPort != 0 {
			if *in.HostPort < minHostPort || *in.HostPort > 65535 {
				p.add(field+".host_port", "must be 0 (the node chooses) or between %d and 65535: lower ports are privileged", minHostPort)
			} else {
				port.HostPort = *in.HostPort
			}
		}
		if port.ContainerPort != 0 {
			if key := (slot{port.ContainerPort, port.Protocol}); containerSeen[key] {
				p.add(field+".container_port", "port %d/%s is listed twice", port.ContainerPort, port.Protocol)
			} else {
				containerSeen[key] = true
			}
		}
		if port.HostPort != 0 {
			if key := (slot{port.HostPort, port.Protocol}); hostSeen[key] {
				p.add(field+".host_port", "host port %d/%s is used twice", port.HostPort, port.Protocol)
			} else {
				hostSeen[key] = true
			}
		}
		out = append(out, port)
	}
	// A canonical order, so listing the same ports differently is not a change.
	sort.Slice(out, func(i, j int) bool {
		if out[i].ContainerPort != out[j].ContainerPort {
			return out[i].ContainerPort < out[j].ContainerPort
		}
		return out[i].Protocol < out[j].Protocol
	})
	return out
}

// intInRange returns v, or def when v is 0 (omitted), and reports values outside [lo, hi].
func intInRange(p *problems, field string, v, lo, hi, def int) int {
	switch {
	case v == 0:
		return def
	case v < lo || v > hi:
		p.add(field, "must be between %d and %d (leave it out for %d)", lo, hi, def)
		return def
	}
	return v
}

func validateHealth(p *problems, h *healthBody) *store.HealthCheck {
	if h == nil {
		return nil
	}
	const field = "spec.health_check"
	out := store.HealthCheck{
		IntervalSeconds:    intInRange(p, field+".interval_seconds", h.IntervalSeconds, 1, maxHealthIntervalSeconds, defaultHealthInterval),
		TimeoutSeconds:     intInRange(p, field+".timeout_seconds", h.TimeoutSeconds, 1, maxHealthTimeoutSeconds, defaultHealthTimeout),
		StartPeriodSeconds: intInRange(p, field+".start_period_seconds", h.StartPeriodSeconds, 0, maxHealthStartPeriod, 0),
		Retries:            intInRange(p, field+".retries", h.Retries, 1, maxHealthRetries, defaultHealthRetries),
	}
	switch {
	case len(h.Command) == 0:
		p.add(field+".command", "is required: the program to run inside the container and its arguments")
	case validArgs(p, field+".command", h.Command, maxHealthArgs, maxHealthArgBytes):
		switch h.Command[0] {
		case "CMD", "CMD-SHELL", "NONE":
			p.add(field+".command", "must start with the program to run, not %q: only the exec form is supported, never a shell string", h.Command[0])
		default:
			out.Command = append([]string(nil), h.Command...)
		}
	}
	return &out
}

func validateRestart(p *problems, r *restartBody) *store.RestartPolicy {
	if r == nil {
		return &store.RestartPolicy{Condition: store.RestartOnFailure, MaxRetries: defaultMaxRetries}
	}
	const field = "spec.restart_policy"
	var out store.RestartPolicy
	switch r.Condition {
	case "":
		p.add(field+".condition", `is required: "no", "on-failure" or "always"`)
	case store.RestartNo:
		out.Condition = r.Condition
		for _, f := range []struct {
			name  string
			value int
		}{{"max_retries", r.MaxRetries}, {"backoff_seconds", r.BackoffSeconds}, {"max_backoff_seconds", r.MaxBackoffSeconds}} {
			if f.value != 0 {
				p.add(field+"."+f.name, `does not apply to the "no" condition: the container is never restarted`)
			}
		}
	case store.RestartAlways:
		// No limit unless one is given.
		out.Condition = r.Condition
		if r.MaxRetries != 0 {
			out.MaxRetries = intInRange(p, field+".max_retries", r.MaxRetries, 1, maxRestartRetries, 0)
		}
		out.BackoffSeconds, out.MaxBackoffSeconds = validateBackoff(p, field, r)
	case store.RestartOnFailure:
		out.Condition = r.Condition
		out.MaxRetries = intInRange(p, field+".max_retries", r.MaxRetries, 1, maxRestartRetries, defaultMaxRetries)
		out.BackoffSeconds, out.MaxBackoffSeconds = validateBackoff(p, field, r)
	default:
		p.add(field+".condition", `must be "no", "on-failure" or "always"`)
	}
	return &out
}

// validateBackoff checks the wait between restarts: it starts at backoff_seconds
// (default 1), doubles with each restart in a row and stops at
// max_backoff_seconds (default 60). A value that is the default is stored as 0,
// so that writing the default and leaving it out are the same definition.
func validateBackoff(p *problems, field string, r *restartBody) (base, limit int) {
	base = intInRange(p, field+".backoff_seconds", r.BackoffSeconds, 1, maxBackoffSeconds, defaultBackoffSeconds)
	limit = intInRange(p, field+".max_backoff_seconds", r.MaxBackoffSeconds, 1, maxBackoffSeconds, defaultMaxBackoffSeconds)
	if base > limit {
		p.add(field+".max_backoff_seconds", "must be at least backoff_seconds (%d)", base)
	}
	if base == defaultBackoffSeconds {
		base = 0
	}
	if limit == defaultMaxBackoffSeconds {
		limit = 0
	}
	return base, limit
}

func validateSelector(p *problems, sel map[string]string) map[string]string {
	if len(sel) == 0 {
		return nil
	}
	if len(sel) > maxSelectorEntries {
		p.add("spec.node_selector", "at most %d labels are allowed", maxSelectorEntries)
		return nil
	}
	ok := true
	for _, k := range sortedKeys(sel) {
		field := fmt.Sprintf("spec.node_selector[%q]", k)
		if !selectorKey.MatchString(k) {
			p.add(field, "the label name must be 1-%d characters of [A-Za-z0-9._/-] starting with a letter or digit", maxSelectorLength)
			ok = false
		}
		if v := sel[k]; len(v) > maxSelectorLength || strings.ContainsFunc(v, unicode.IsControl) {
			p.add(field, "the label value must be at most %d characters without control characters", maxSelectorLength)
			ok = false
		}
	}
	if !ok {
		return nil
	}
	out := make(map[string]string, len(sel))
	for k, v := range sel {
		out[k] = v
	}
	return out
}

// parseIfMatch reads an If-Match header: "N" (or N) names a version, * or no
// header means any. ok is false for anything else.
func parseIfMatch(header string) (version int64, ok bool) {
	header = strings.TrimSpace(header)
	if header == "" || header == "*" {
		return 0, true
	}
	v, err := strconv.ParseInt(strings.Trim(header, `"`), 10, 64)
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}
