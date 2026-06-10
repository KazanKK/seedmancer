package cmd

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// adHocEnvName is the internal sentinel used when the target URL came from
// --db-url or $SEEDMANCER_DATABASE_URL instead of a named env.
const adHocEnvName = "(ad-hoc)"

// targetDisplay returns a human-readable label for a target environment.
// For named envs it returns the env name. For ad-hoc DB URLs it returns
// host:port/dbname (credentials stripped) so prompts read naturally.
func targetDisplay(t utils.NamedEnv) string {
	if t.Name != adHocEnvName {
		return t.Name
	}
	u, err := url.Parse(t.DatabaseURL)
	if err != nil || u.Host == "" {
		return t.DatabaseURL
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return u.Host
	}
	return u.Host + "/" + db
}

// resolveSingleDB picks one database target for commands that write to a
// single DB (export, status). Precedence, highest first:
//
//  1. --db-url flag (explicit ad-hoc override)
//  2. --env <name> or cfg.DefaultEnv (named env from seedmancer.yaml)
//  3. $SEEDMANCER_DATABASE_URL — only used when no environments are
//     configured (bare CI / no seedmancer.yaml scenario)
//
// $SEEDMANCER_DATABASE_URL is intentionally last so that a project with
// a configured default_env always resolves to its named environment instead
// of being silently overridden by an ambient variable.
func resolveSingleDB(c *cli.Context, cfg utils.Config) (utils.NamedEnv, error) {
	if adhoc := strings.TrimSpace(c.String("db-url")); adhoc != "" {
		return utils.NamedEnv{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: adhoc},
		}, nil
	}
	if len(cfg.EffectiveEnvs()) == 0 {
		if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" {
			return utils.NamedEnv{
				Name:      adHocEnvName,
				EnvConfig: utils.EnvConfig{DatabaseURL: v},
			}, nil
		}
	}
	return cfg.ResolveEnv(c.String("env"))
}

// resolveSeedTargets parses the multi-target --env flag for `seed`.
//
// Rules that make this feel right in practice:
//   - --db-url is the single-target escape hatch; it short-circuits --env
//     and produces one ad-hoc target (otherwise `--db-url x --env local,staging`
//     is ambiguous).
//   - $SEEDMANCER_DATABASE_URL is only used when no environments are configured
//     (bare CI scenario), so a project with named envs always resolves cleanly.
//   - An empty --env falls back to the active default env, so `seedmancer
//     seed snap1` keeps working for users who never adopt named envs.
func resolveSeedTargets(c *cli.Context, cfg utils.Config) ([]utils.NamedEnv, error) {
	if adhoc := strings.TrimSpace(c.String("db-url")); adhoc != "" {
		if c.IsSet("env") {
			return nil, fmt.Errorf("--db-url and --env are mutually exclusive")
		}
		return []utils.NamedEnv{{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: adhoc},
		}}, nil
	}
	if len(cfg.EffectiveEnvs()) == 0 && !c.IsSet("env") {
		if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" {
			return []utils.NamedEnv{{
				Name:      adHocEnvName,
				EnvConfig: utils.EnvConfig{DatabaseURL: v},
			}}, nil
		}
	}
	return cfg.ResolveEnvs(c.String("env"))
}

// isProdLike decides whether a "you're about to touch prod" confirmation
// should fire. Matching is case-insensitive and covers the names real
// teams actually use (`prod`, `production`, `live`). When users pass
// `--yes` we skip the prompt; the function only decides whether to ask.
func isProdLike(envName string) bool {
	switch strings.ToLower(strings.TrimSpace(envName)) {
	case "prod", "production", "live":
		return true
	}
	return false
}
