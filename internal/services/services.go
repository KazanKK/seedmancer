// Package services defines the Connector interface and factory for Seedmancer's
// 3rd-party service connectors (Supabase Auth, …).
//
// Each connector can snapshot and restore an external service's test state
// alongside a Postgres dataset so that `seedmancer seed` resets the entire
// test environment in one command.
package services

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/KazanKK/seedmancer/internal/utils"
)

// dbURLContextKey is used to stash the Postgres DB URL in a context so
// service connectors can open a direct DB connection when they need to
// perform auxiliary cleanup (e.g. removing auth-mirror rows before triggering
// an auth.users INSERT).
type dbURLContextKey struct{}

// WithDBURL returns a child context that carries the given Postgres URL.
// The auth connector extracts it to handle ON CONFLICT situations caused
// by application-level INSERT triggers on auth.users.
func WithDBURL(ctx context.Context, dbURL string) context.Context {
	return context.WithValue(ctx, dbURLContextKey{}, dbURL)
}

// DBURLFromContext extracts the Postgres URL stored by WithDBURL.
// Returns ("", false) when not present.
func DBURLFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(dbURLContextKey{}).(string)
	return v, ok && v != ""
}

// Connector is the interface every 3rd-party service connector must implement.
type Connector interface {
	// ServiceType returns the canonical type string (e.g. "supabase-auth").
	ServiceType() string

	// Export captures the current state of the service and returns it as
	// opaque JSON bytes that are written to the dataset folder as
	// _<serviceName>.json.
	Export(ctx context.Context) ([]byte, error)

	// Seed wipes the service's test state and restores it from the snapshot
	// bytes previously produced by Export.
	Seed(ctx context.Context, snapshot []byte) error
}

// SidecarFilename returns the dataset-folder sidecar filename for a service
// connector named name (e.g. "auth" → "_auth.json").
func SidecarFilename(name string) string {
	return "_" + name + ".json"
}

// New instantiates the Connector for cfg. The name argument is the key from
// seedmancer.yaml's services map (used in error messages only). Credentials
// are read at construction time from the environment variables named in cfg;
// this returns an error if a required variable is missing or empty.
func New(name string, cfg utils.ServiceConfig) (Connector, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "supabase-auth":
		return newSupabaseAuth(name, cfg)
	default:
		return nil, fmt.Errorf(
			"service %q: unknown type %q (supported: supabase-auth)",
			name, cfg.Type,
		)
	}
}

// BuildAll resolves all services in the config into an ordered slice of
// (name, Connector) pairs. Order is alphabetical by name so export and seed
// runs are deterministic. Services whose env-var credentials are missing cause
// a descriptive error that names the offending variable and service.
func BuildAll(cfg utils.Config) ([]NamedConnector, error) {
	names := cfg.SortedServiceNames()
	out := make([]NamedConnector, 0, len(names))
	for _, name := range names {
		svcCfg := cfg.Services[name]
		c, err := New(name, svcCfg)
		if err != nil {
			return nil, err
		}
		out = append(out, NamedConnector{Name: name, Connector: c})
	}
	return out, nil
}

// NamedConnector pairs a connector with the user-chosen name from
// seedmancer.yaml (e.g. "auth"). The name is used for sidecar
// filenames and progress messages.
type NamedConnector struct {
	Name      string
	Connector Connector
}

// SidecarFilename returns the on-disk filename for this connector's snapshot.
func (n NamedConnector) SidecarFilename() string {
	return SidecarFilename(n.Name)
}

// SortedNames returns the service names in alphabetical order (deterministic
// for tests and CLI output). Exported to let callers avoid importing sort.
func SortedNames(services map[string]utils.ServiceConfig) []string {
	out := make([]string, 0, len(services))
	for k := range services {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ─── credential helpers ──────────────────────────────────────────────────────

// resolveValue resolves a credential field that may be either:
//   - a direct value (URL starting with http:// or https://, or a JWT
//     starting with eyJ), or
//   - an environment variable name whose value should be looked up.
//
// This allows seedmancer.yaml to hold the literal value for convenience (e.g.
// url_env: http://host.docker.internal:54321) while still supporting the
// intended pattern of pointing at an env var (url_env: SUPABASE_URL).
// Users who commit seedmancer.yaml to git should use env var names so that
// secrets are not stored in the repo.
func resolveValue(serviceName, fieldName, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("service %q: %s is required", serviceName, fieldName)
	}
	// Detect direct values.
	if strings.HasPrefix(raw, "http://") ||
		strings.HasPrefix(raw, "https://") ||
		strings.HasPrefix(raw, "eyJ") { // JWT / Bearer token
		return raw, nil
	}
	// Treat as an env var name.
	v := strings.TrimSpace(os.Getenv(raw))
	if v == "" {
		return "", fmt.Errorf(
			"service %q: %s references env var %s which is not set (hint: export %s=<value>)",
			serviceName, fieldName, raw, raw,
		)
	}
	return v, nil
}
