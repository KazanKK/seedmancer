package cmd

import (
	"fmt"
	"os"
	"strings"

	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// adHocEnvName is the label used in banners/errors when the target URL came
// from --db-url or $SEEDMANCER_DATABASE_URL instead of a named env. Keeping
// it in one place means every command prints the same string.
const adHocEnvName = "(ad-hoc)"

// resolveSingleDB picks one database target for commands that write to a
// single DB (export, status). Precedence, highest first:
//
//  1. --db-url flag / $SEEDMANCER_DATABASE_URL env (ad-hoc override)
//  2. --env <name> (named env from seedmancer.yaml)
//  3. cfg.DefaultEnv (the "set and forget" path)
//  4. legacy top-level cfg.DatabaseURL (surfaced as env "default")
//
// The returned NamedEnv.Name is "(ad-hoc)" when #1 wins so downstream code
// can still print "→ using env: (ad-hoc)" without a special case.
func resolveSingleDB(c *cli.Context, cfg utils.Config) (utils.NamedEnv, error) {
	if adhoc := strings.TrimSpace(c.String("db-url")); adhoc != "" {
		return utils.NamedEnv{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: adhoc},
		}, nil
	}
	if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" {
		return utils.NamedEnv{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: v},
		}, nil
	}
	return cfg.ResolveEnv(c.String("env"))
}

// resolveSeedTargets parses the multi-target --env flag for `seed`.
//
// Rules that make this feel right in practice:
//   - --db-url is the single-target escape hatch; it short-circuits --env
//     and produces one ad-hoc target (otherwise `--db-url x --env local,staging`
//     is ambiguous).
//   - An empty --env falls back to the active default env, so `seedmancer
//     seed -d snap1` keeps working for users who never adopt named envs.
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
	if v := strings.TrimSpace(os.Getenv("SEEDMANCER_DATABASE_URL")); v != "" && !c.IsSet("env") {
		return []utils.NamedEnv{{
			Name:      adHocEnvName,
			EnvConfig: utils.EnvConfig{DatabaseURL: v},
		}}, nil
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
