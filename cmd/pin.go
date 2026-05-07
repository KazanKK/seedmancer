package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KazanKK/seedmancer/internal/scenario"
	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"

	"github.com/urfave/cli/v2"
)

// PinCommand sets the scenario's pointers.stable. With no flags it pins
// the current latest revision; --revision pins a specific one (validated
// for existence first).
func PinCommand() *cli.Command {
	return &cli.Command{
		Name:      "pin",
		Usage:     "Mark a scenario revision as stable",
		ArgsUsage: "<scenario>",
		Description: "Updates pointers.stable so `seedmancer seed <scenario> --stable`\n" +
			"reproduces the same dataset.\n\n" +
			"  seedmancer pin billing/pro             # pin latest as stable\n" +
			"  seedmancer pin billing/pro -r r002     # pin a specific revision",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "revision",
				Aliases: []string{"r"},
				Usage:   "Revision id to pin (defaults to the current latest)",
			},
		},
		Action: func(c *cli.Context) error {
			scenarioArg := strings.TrimSpace(c.Args().First())
			if scenarioArg == "" {
				return fmt.Errorf("usage: seedmancer pin <scenario>")
			}
			out, err := RunPin(c.Context, PinInput{
				Scenario: scenarioArg,
				Revision: strings.TrimSpace(c.String("revision")),
			})
			if err != nil {
				return err
			}
			ui.Success("Pinned %s %s as stable", out.Scenario, out.Revision)
			return nil
		},
	}
}

// PinInput is the structured input for RunPin.
type PinInput struct {
	Scenario string `json:"scenario" jsonschema:"Scenario path"`
	Revision string `json:"revision,omitempty" jsonschema:"Specific revision id; defaults to the current latest"`
}

// PinOutput is the structured response for RunPin.
type PinOutput struct {
	Scenario string `json:"scenario"`
	Revision string `json:"revision"`
}

// RunPin updates pointers.stable. Missing revision → friendly error
// (rather than silently writing a dangling pointer).
func RunPin(_ context.Context, in PinInput) (PinOutput, error) {
	configPath, err := utils.FindConfigFile()
	if err != nil {
		return PinOutput{}, err
	}
	cfg, err := utils.LoadConfig(configPath)
	if err != nil {
		return PinOutput{}, err
	}
	projectRoot := filepath.Dir(configPath)

	scenarioPath, err := scenario.Normalize(in.Scenario)
	if err != nil {
		return PinOutput{}, err
	}
	scenarioDir := scenario.ScenarioDir(projectRoot, cfg.StoragePath, scenarioPath)
	if _, err := os.Stat(scenarioDir); os.IsNotExist(err) {
		return PinOutput{}, fmt.Errorf("scenario %q does not exist", scenarioPath)
	}
	pointers, _ := scenario.ReadPointers(scenarioDir)

	revID := strings.TrimSpace(in.Revision)
	if revID == "" {
		if pointers.Latest == "" {
			return PinOutput{}, fmt.Errorf(
				"scenario %q has no revisions yet — run `seedmancer export %s` first",
				scenarioPath, scenarioPath,
			)
		}
		revID = pointers.Latest
	}

	revDir := scenario.RevisionDir(projectRoot, cfg.StoragePath, scenarioPath, revID)
	if st, err := os.Stat(revDir); err != nil || !st.IsDir() {
		return PinOutput{}, fmt.Errorf("revision %q does not exist under scenario %q", revID, scenarioPath)
	}

	pointers.Stable = revID
	if err := scenario.WritePointers(scenarioDir, pointers); err != nil {
		return PinOutput{}, err
	}

	manifest, err := scenario.ReadManifest(scenarioDir)
	if err == nil {
		manifest.StableRevision = revID
		_ = scenario.WriteManifest(scenarioDir, manifest)
	}

	return PinOutput{Scenario: scenarioPath, Revision: revID}, nil
}
