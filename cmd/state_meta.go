package cmd

import (
	"context"
	"path/filepath"
	"sort"
	"time"

	"github.com/KazanKK/seedmancer/internal/contract"
	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/usage"
	utils "github.com/KazanKK/seedmancer/internal/utils"
)

// This file holds the stdout-free runners behind the state-metadata MCP
// tools: inspecting which Playwright tests use which states, and authoring a
// state's contract.yaml. They share the same local on-disk metadata the CLI
// surfaces via `seedmancer list --usage` and `seedmancer check <state>`.

// ─── state usage ────────────────────────────────────────────────────────────

// StateUsageInput optionally narrows the report to one state.
type StateUsageInput struct {
	State string `json:"state,omitempty" jsonschema:"Limit the report to a single state path (e.g. auth/login-success); omit for all states"`
}

// StateUsageEntry is the aggregated usage for one state.
type StateUsageEntry struct {
	State      string      `json:"state"`
	UsedBy     []usage.Ref `json:"usedBy"`
	LastUsedAt time.Time   `json:"lastUsedAt,omitempty"`
}

// StateUsageOutput is the structured response for RunStateUsage.
type StateUsageOutput struct {
	States []StateUsageEntry `json:"states"`
}

// RunStateUsage aggregates the per-test usage events and returns which tests
// use each state. It also persists the derived state-usage.json cache. When
// in.State is set, only that state is returned (empty UsedBy if never used).
func RunStateUsage(_ context.Context, in StateUsageInput) (StateUsageOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return StateUsageOutput{States: []StateUsageEntry{}}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return StateUsageOutput{States: []StateUsageEntry{}}, err
	}
	projectRoot := filepath.Dir(configPath)

	agg, err := usage.Load(projectRoot, cfg.StoragePath)
	if err != nil {
		return StateUsageOutput{States: []StateUsageEntry{}}, err
	}
	_ = usage.Persist(projectRoot, cfg.StoragePath, agg)

	var filter string
	if in.State != "" {
		if norm, nerr := scenario.Normalize(in.State); nerr == nil {
			filter = norm
		} else {
			filter = in.State
		}
	}

	out := StateUsageOutput{States: []StateUsageEntry{}}
	if filter != "" {
		su := agg.States[filter]
		entry := StateUsageEntry{State: filter, UsedBy: []usage.Ref{}}
		if su != nil {
			entry.UsedBy = su.UsedBy
			entry.LastUsedAt = su.LastUsedAt
		}
		out.States = append(out.States, entry)
		return out, nil
	}

	for state, su := range agg.States {
		out.States = append(out.States, StateUsageEntry{
			State:      state,
			UsedBy:     su.UsedBy,
			LastUsedAt: su.LastUsedAt,
		})
	}
	sort.Slice(out.States, func(i, j int) bool { return out.States[i].State < out.States[j].State })
	return out, nil
}

// ─── state contract ──────────────────────────────────────────────────────────

// WriteContractInput authors (or overwrites) a state's contract.yaml.
type WriteContractInput struct {
	State    string                            `json:"state" jsonschema:"State (scenario) path the contract belongs to"`
	Purpose  string                            `json:"purpose,omitempty" jsonschema:"One-line description of what the state means"`
	Provides map[string]map[string]interface{} `json:"provides,omitempty" jsonschema:"Named data handles. Keys ending in 'Env' are resolved from the environment at test time (e.g. passwordEnv: SEEDMANCER_TEST_PASSWORD)"`
	MustHave []string                          `json:"mustHave,omitempty" jsonschema:"Human-readable invariants the state must satisfy"`
}

// WriteContractOutput reports where the contract was written.
type WriteContractOutput struct {
	State string `json:"state"`
	Path  string `json:"path"`
}

// RunWriteContract writes a contract.yaml alongside the named state.
func RunWriteContract(_ context.Context, in WriteContractInput) (WriteContractOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return WriteContractOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return WriteContractOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	statePath, err := scenario.Normalize(in.State)
	if err != nil {
		return WriteContractOutput{}, err
	}

	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, statePath)
	c := contract.Contract{
		State:    statePath,
		Purpose:  in.Purpose,
		Provides: in.Provides,
		MustHave: in.MustHave,
	}
	if err := contract.Write(scenarioDir, c); err != nil {
		return WriteContractOutput{}, err
	}

	return WriteContractOutput{State: statePath, Path: contract.Path(scenarioDir)}, nil
}
