package protocol

import (
	"fmt"

	dockfleetv1 "github.com/dockfleet/dockfleet/internal/protocol/dockfleet/v1"
)

// The protocol version this build speaks. Bump VersionMinor for backward
// compatible additions and VersionMajor for breaking changes.
const (
	VersionMajor uint32 = 1
	VersionMinor uint32 = 1
)

// CurrentVersion returns a fresh message holding this build's protocol version.
func CurrentVersion() *dockfleetv1.ProtocolVersion {
	return &dockfleetv1.ProtocolVersion{Major: VersionMajor, Minor: VersionMinor}
}

func formatVersion(v *dockfleetv1.ProtocolVersion) string {
	if v == nil {
		return "unknown"
	}
	return fmt.Sprintf("%d.%d", v.GetMajor(), v.GetMinor())
}

// checkAgentVersion is the manager's rule: same major, and the agent must not
// be newer than the manager because it could rely on features the manager lacks.
func checkAgentVersion(manager, agent *dockfleetv1.ProtocolVersion) error {
	if agent == nil {
		return fmt.Errorf("agent did not send a protocol version; manager speaks %s", formatVersion(manager))
	}
	if agent.GetMajor() != manager.GetMajor() {
		return fmt.Errorf("agent protocol %s is incompatible with manager protocol %s", formatVersion(agent), formatVersion(manager))
	}
	if agent.GetMinor() > manager.GetMinor() {
		return fmt.Errorf("agent protocol %s is newer than manager protocol %s; upgrade the manager first", formatVersion(agent), formatVersion(manager))
	}
	return nil
}

// checkManagerVersion is the agent's rule: same major. A newer manager minor
// only adds features.
func checkManagerVersion(agent, manager *dockfleetv1.ProtocolVersion) error {
	if manager == nil || manager.GetMajor() != agent.GetMajor() {
		return fmt.Errorf("manager protocol %s is incompatible with agent protocol %s", formatVersion(manager), formatVersion(agent))
	}
	return nil
}
