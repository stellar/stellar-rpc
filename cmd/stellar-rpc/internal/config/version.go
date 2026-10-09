//nolint:gochecknoglobals // allow global variables
package config

var (
	// Version is the stellar-rpc version number, which is injected during build time.
	Version = "0.0.0"

	// CommitHash is the stellar-rpc git commit hash, which is injected during build time.
	CommitHash = ""

	// BuildTimestamp is the timestamp at which the stellar-rpc was built, injected during build time.
	BuildTimestamp = ""

	// Branch is the git branch from which the stellar-rpc was built, injected during build time.
	Branch = ""

	// RSSorobanEnvVersionCurr is the current rs-soroban-env version, injected during build time.
	RSSorobanEnvVersionCurr = ""
)
