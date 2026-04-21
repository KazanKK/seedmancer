package cmd

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// statusReport is the JSON shape of `seedmancer status --json`. It's kept
// flat and boring on purpose: CI pipelines grep this output to decide
// whether to run destructive commands, so we avoid nested envelopes or
// "maybe populated" fields where possible.
type statusReport struct {
	Project struct {
		ConfigPath  string `json:"configPath,omitempty"`
		ConfigScope string `json:"configScope"`
		StoragePath string `json:"storagePath,omitempty"`
		DatabaseURL string `json:"databaseUrl,omitempty"`
	} `json:"project"`
	API struct {
		URL    string `json:"url"`
		Source string `json:"source"`
	} `json:"api"`
	Auth struct {
		SignedIn       bool   `json:"signedIn"`
		Source         string `json:"source"`
		TokenFingerpr  string `json:"tokenFingerprint,omitempty"`
		ShadowedByEnv  bool   `json:"shadowedByEnv"`
		EnvTokenInUse  bool   `json:"envTokenInUse"`
		Reachable      *bool  `json:"reachable,omitempty"`
		ReachableError string `json:"reachableError,omitempty"`
	} `json:"auth"`
	Schemas struct {
		LocalCount int `json:"localCount"`
	} `json:"schemas"`
}

// tokenSource captures both where the active token came from and the raw
// value (so we can derive a masked fingerprint for display) without
// leaking the token anywhere else in the program.
type tokenSource struct {
	Source string // "env", "credentials", "config", or "" when unauthenticated
	Token  string
}

// StatusCommand renders a single, self-describing overview of how the CLI
// will behave right now: which seedmancer.yaml it picks up, the effective
// API URL, whether it's signed in (and from where), plus a one-shot
// reachability probe. Running it first is the fastest way to understand
// "why isn't command X working?" without guessing which env var or config
// key wins.
func StatusCommand() *cli.Command {
	return &cli.Command{
		Name:      "status",
		Usage:     "Show current configuration, auth, and API reachability",
		ArgsUsage: " ",
		Description: "Prints the effective configuration the CLI is using right now:\n" +
			"which seedmancer.yaml was picked up, the API URL (and whether it\n" +
			"came from env / config / default), whether you're signed in and\n" +
			"through which source, and a masked preview of the active token.\n\n" +
			"By default also performs a lightweight reachability check against\n" +
			"the API. Pass --offline to skip the network call, or --json for a\n" +
			"machine-readable snapshot.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "offline",
				Usage: "Skip the API reachability check",
			},
			&cli.BoolFlag{
				Name:  "show-db-url",
				Usage: "Show database_url with credentials (default masks the password)",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "Emit result as JSON for CI/CD pipelines",
			},
		},
		Action: runStatus,
	}
}

func runStatus(c *cli.Context) error {
	report := buildStatusReport(c.Bool("show-db-url"))

	if !c.Bool("offline") && report.Auth.SignedIn {
		ok, errMsg := probeAPIReachable(report.API.URL, resolveActiveTokenForProbe())
		report.Auth.Reachable = &ok
		if !ok {
			report.Auth.ReachableError = errMsg
		}
	}

	if c.Bool("json") {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}

	renderStatus(report)
	return nil
}

// buildStatusReport gathers the pieces that don't need network access.
// It never reads the token value into the rendered report directly —
// only a masked fingerprint — so accidentally pasting CLI output into
// chat/issues is safe.
func buildStatusReport(showDBURL bool) statusReport {
	var report statusReport

	configPath, cfgErr := utils.FindConfigFile()
	if cfgErr == nil {
		report.Project.ConfigPath = configPath
		report.Project.ConfigScope = classifyConfigScope(configPath)
		if cfg, err := utils.LoadConfig(configPath); err == nil {
			report.Project.StoragePath = cfg.StoragePath
			if cfg.DatabaseURL != "" {
				if showDBURL {
					report.Project.DatabaseURL = cfg.DatabaseURL
				} else {
					report.Project.DatabaseURL = maskDatabaseURL(cfg.DatabaseURL)
				}
			}
		}
	} else {
		report.Project.ConfigScope = "none"
	}

	report.API.URL, report.API.Source = resolveAPIURLSource()

	src := locateActiveToken()
	if src.Token != "" {
		report.Auth.SignedIn = true
		report.Auth.Source = src.Source
		report.Auth.TokenFingerpr = maskToken(src.Token)
		report.Auth.EnvTokenInUse = src.Source == "env"

		// Flag the case where the credentials file is authoritative but
		// SEEDMANCER_API_TOKEN is also set. Nothing is broken — the
		// credentials file wins — but users should know the env var will
		// only matter if they `seedmancer logout` or delete the file.
		envTok := strings.TrimSpace(os.Getenv("SEEDMANCER_API_TOKEN"))
		if envTok != "" && src.Source == "credentials" && envTok != src.Token {
			report.Auth.ShadowedByEnv = true
		}
	} else {
		report.Auth.Source = "none"
	}

	if projectRoot, err := projectRootForStatus(); err == nil {
		storage := report.Project.StoragePath
		if storage == "" {
			storage = ".seedmancer"
		}
		if schemas, err := utils.ListLocalSchemas(projectRoot, storage); err == nil {
			report.Schemas.LocalCount = len(schemas)
		}
	}

	return report
}

func renderStatus(r statusReport) {
	ui.Title("Seedmancer status")

	ui.Title("Project")
	if r.Project.ConfigPath != "" {
		ui.KeyValue("config:       ", fmt.Sprintf("%s (%s)", r.Project.ConfigPath, r.Project.ConfigScope))
	} else {
		ui.KeyValue("config:       ", "none — run `seedmancer init`")
	}
	if r.Project.StoragePath != "" {
		ui.KeyValue("storage_path: ", r.Project.StoragePath)
	}
	if r.Project.DatabaseURL != "" {
		ui.KeyValue("database_url: ", r.Project.DatabaseURL)
	} else {
		ui.KeyValue("database_url: ", "(unset — pass --db-url or set SEEDMANCER_DATABASE_URL)")
	}
	ui.KeyValue("local schemas:", fmt.Sprintf("%d", r.Schemas.LocalCount))

	ui.Title("API")
	ui.KeyValue("url:          ", fmt.Sprintf("%s (%s)", r.API.URL, r.API.Source))

	ui.Title("Auth")
	if r.Auth.SignedIn {
		ui.KeyValue("signed in:    ", "yes")
		ui.KeyValue("token source: ", humanizeTokenSource(r.Auth.Source))
		ui.KeyValue("token:        ", r.Auth.TokenFingerpr)
		if r.Auth.Reachable != nil {
			if *r.Auth.Reachable {
				ui.KeyValue("reachable:    ", "yes")
			} else {
				ui.KeyValue("reachable:    ", fmt.Sprintf("no — %s", r.Auth.ReachableError))
			}
		}
		if r.Auth.ShadowedByEnv {
			fmt.Println()
			ui.Warn("SEEDMANCER_API_TOKEN is set in your shell but is being ignored — the credentials file takes precedence.")
			ui.Info("The env var will only be used after `seedmancer logout`. To clear it now:  unset SEEDMANCER_API_TOKEN")
		}
	} else {
		ui.KeyValue("signed in:    ", "no")
		fmt.Println()
		ui.PrintLoginHint()
	}
}

// classifyConfigScope tags the picked-up config as a project-level file
// (seedmancer.yaml next to the project) or the per-user fallback under
// ~/.seedmancer. The status output uses this to make it obvious which
// file actually won, since users are frequently surprised by the
// upward-walk fallback.
func classifyConfigScope(path string) string {
	home, err := os.UserHomeDir()
	if err == nil {
		global := filepath.Join(home, ".seedmancer", "config.yaml")
		if path == global {
			return "global"
		}
	}
	return "project"
}

// resolveAPIURLSource returns (value, source) where source is one of:
// "env", "config", or "default". It intentionally mirrors the order in
// utils.GetBaseURL so the two never drift.
func resolveAPIURLSource() (string, string) {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("SEEDMANCER_API_URL")), "/"); v != "" {
		return v, "env"
	}
	if cfgPath, err := utils.FindConfigFile(); err == nil {
		if cfg, err := utils.LoadConfig(cfgPath); err == nil && cfg.APIURL != "" {
			return strings.TrimRight(cfg.APIURL, "/"), "config"
		}
	}
	return "https://api.seedmancer.dev", "default"
}

// locateActiveToken reproduces utils.ResolveAPIToken's priority order
// but also records *where* the token came from so status can show it.
// Credentials file beats env var beats legacy config — matching
// ResolveAPIToken. Keep these two in lockstep if you change one.
func locateActiveToken() tokenSource {
	if tok, err := utils.LoadAPICredentials(); err == nil && tok != "" {
		return tokenSource{Source: "credentials", Token: tok}
	}
	if v := strings.TrimSpace(os.Getenv("SEEDMANCER_API_TOKEN")); v != "" {
		return tokenSource{Source: "env", Token: v}
	}
	if cfgPath, err := utils.FindConfigFile(); err == nil {
		if cfg, err := utils.LoadConfig(cfgPath); err == nil && cfg.APIToken != "" {
			return tokenSource{Source: "config", Token: cfg.APIToken}
		}
	}
	return tokenSource{}
}

// resolveActiveTokenForProbe is the no-source variant used when we only
// need the token value itself (for the reachability HTTP call).
func resolveActiveTokenForProbe() string { return locateActiveToken().Token }

func humanizeTokenSource(s string) string {
	switch s {
	case "env":
		return "SEEDMANCER_API_TOKEN (env var)"
	case "credentials":
		path, err := utils.CredentialsPath()
		if err != nil {
			return "~/.seedmancer/credentials"
		}
		return path
	case "config":
		if cfgPath, err := utils.FindConfigFile(); err == nil {
			return fmt.Sprintf("%s (legacy api_token)", cfgPath)
		}
		return "seedmancer.yaml (legacy api_token)"
	default:
		return s
	}
}

// maskToken returns a shape like "sk_1eab…f5c3 (len 64)" that carries
// enough entropy for a human to verify they're looking at the right
// credential without exposing the full secret.
func maskToken(tok string) string {
	tok = strings.TrimSpace(tok)
	if len(tok) <= 8 {
		return fmt.Sprintf("***** (len %d)", len(tok))
	}
	return fmt.Sprintf("%s…%s (len %d)", tok[:4], tok[len(tok)-4:], len(tok))
}

// maskDatabaseURL strips the password from a URL while keeping everything
// else a user would want to eyeball (host, port, db name, query flags).
// Invalid URLs fall through unmodified — they're already noisy and
// users need to see the raw value to fix them.
//
// We don't use url.UserPassword here because the net/url package
// percent-encodes special characters (so "****" comes out as "%2A%2A%2A%2A"),
// which obscures what we're trying to communicate. Clearing the password
// via SetPassword("") and splicing a literal mask keeps the output
// readable at a glance.
func maskDatabaseURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	user := u.User.Username()
	if _, hasPwd := u.User.Password(); !hasPwd {
		return raw
	}
	u.User = url.User(user)
	s := u.String()
	// Inject the mask after the username. u.String() emits
	// scheme://user@host; we want scheme://user:****@host.
	needle := user + "@"
	if idx := strings.Index(s, needle); idx >= 0 {
		return s[:idx+len(user)] + ":****" + s[idx+len(user):]
	}
	return s
}

// probeAPIReachable hits a cheap authenticated endpoint to prove end-to-end
// connectivity: DNS, TLS, the user's token, and the API all have to be up
// for this to succeed. We use /v1.0/schemas because it's already wired
// everywhere else and returns quickly even for accounts with lots of
// schemas (it's capped server-side).
func probeAPIReachable(baseURL, token string) (bool, string) {
	if token == "" {
		return false, "no token configured"
	}
	req, err := http.NewRequest("GET", baseURL+"/v1.0/schemas", nil)
	if err != nil {
		return false, err.Error()
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return false, utils.ErrInvalidAPIToken.Error()
	}
	if resp.StatusCode >= 400 {
		return false, fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	return true, ""
}

// projectRootForStatus returns the directory containing the nearest
// seedmancer.yaml, or the current working directory when only the global
// config was found. Used to count local schemas without requiring every
// caller to replicate the walk logic.
func projectRootForStatus() (string, error) {
	cfgPath, err := utils.FindConfigFile()
	if err != nil {
		return os.Getwd()
	}
	if classifyConfigScope(cfgPath) == "global" {
		return os.Getwd()
	}
	return filepath.Dir(cfgPath), nil
}
