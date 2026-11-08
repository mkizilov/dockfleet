package version

// Version is overridden at build time:
// -ldflags "-X github.com/dockfleet/dockfleet/internal/version.Version=<value>".
var Version = "dev"
