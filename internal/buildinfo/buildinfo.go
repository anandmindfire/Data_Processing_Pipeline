// Package buildinfo exposes version metadata injected at build time via -ldflags, e.g.
//
//	go build -ldflags "-X github.com/anandmindfire/data-processing-pipeline/internal/buildinfo.Version=v1.2.3"
package buildinfo

import "runtime"

// Values overridden at link time by the Makefile, Dockerfile, and CI.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info is the serializable build metadata.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
}

// Get returns the build metadata of the running binary.
func Get() Info {
	return Info{Version: Version, Commit: Commit, BuildDate: Date, GoVersion: runtime.Version()}
}
