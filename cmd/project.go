package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// projectAPI mirrors the /v1.0/projects response shape.
type projectAPI struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type projectListResponse struct {
	Projects []projectAPI `json:"projects"`
}

// ProjectCommand exposes list/current/use/create operations for cloud projects.
func ProjectCommand() *cli.Command {
	return &cli.Command{
		Name:            "project",
		Usage:           "Manage cloud projects",
		HideHelpCommand: true,
		Description: "A cloud project scopes all scenarios, schemas, and test data on\n" +
			"the Seedmancer platform. Each account starts with a \"Default\" project.\n" +
			"You can create up to 3 projects and switch between them with\n" +
			"`seedmancer project use <slug>` or the --project flag.",
		Subcommands: []*cli.Command{
			projectListCommand(),
			projectCurrentCommand(),
			projectUseCommand(),
			projectCreateCommand(),
		},
	}
}

func projectListCommand() *cli.Command {
	return &cli.Command{
		Name:      "list",
		Usage:     "List all cloud projects",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token override",
			},
		},
		Action: func(c *cli.Context) error {
			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}
			projects, err := listProjects(token)
			if err != nil {
				return err
			}

			configPath, _ := utils.FindConfigFile()
			var currentSlug string
			if configPath != "" {
				if cfg, cfgErr := utils.LoadConfig(configPath); cfgErr == nil {
					currentSlug = cfg.DefaultProject
				}
			}

			fmt.Fprintln(os.Stderr)
			for _, p := range projects {
				marker := "  "
				if p.Slug == currentSlug {
					marker = " *"
				}
				fmt.Fprintf(os.Stderr, "%s %-20s  %s\n", marker, p.Slug, p.Name)
			}
			fmt.Fprintln(os.Stderr)
			if currentSlug != "" {
				ui.Info("Default: %s  (change with `seedmancer project use <slug>`)", currentSlug)
			} else {
				ui.Info("No default project set — cloud APIs fall back to the Default project.")
				ui.Info("Run `seedmancer project use <slug>` to pin one.")
			}
			return nil
		},
	}
}

func projectCurrentCommand() *cli.Command {
	return &cli.Command{
		Name:      "current",
		Usage:     "Show the active cloud project",
		ArgsUsage: " ",
		Action: func(c *cli.Context) error {
			configPath, err := utils.FindConfigFile()
			if err != nil {
				ui.Info("No seedmancer.yaml found — using server Default project.")
				return nil
			}
			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}
			if cfg.DefaultProject == "" {
				ui.Info("No default project configured — cloud APIs fall back to the Default project.")
				ui.Info("Run `seedmancer project use <slug>` to pin one.")
			} else {
				ui.Info("Active cloud project: %s", cfg.DefaultProject)
			}
			return nil
		},
	}
}

func projectUseCommand() *cli.Command {
	return &cli.Command{
		Name:      "use",
		Usage:     "Set the default cloud project",
		ArgsUsage: "<slug>",
		Action: func(c *cli.Context) error {
			slug := strings.TrimSpace(c.Args().First())
			if slug == "" {
				return usageError(c, "missing required argument: <slug>")
			}

			configPath, err := utils.FindConfigFile()
			if err != nil {
				return fmt.Errorf("no seedmancer.yaml found: %v", err)
			}
			cfg, err := utils.LoadConfig(configPath)
			if err != nil {
				return err
			}
			cfg.DefaultProject = slug
			if err := utils.SaveConfig(configPath, cfg); err != nil {
				return fmt.Errorf("saving config: %v", err)
			}
			ui.Success("Default cloud project set to %q", slug)
			return nil
		},
	}
}

func projectCreateCommand() *cli.Command {
	return &cli.Command{
		Name:      "create",
		Usage:     "Create a new cloud project",
		ArgsUsage: "<name>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "token",
				Usage: "API token override",
			},
		},
		Action: func(c *cli.Context) error {
			name := strings.TrimSpace(c.Args().First())
			if name == "" {
				return usageError(c, "missing required argument: <name>")
			}

			token, err := utils.ResolveAPIToken(c.String("token"))
			if err != nil {
				return err
			}

			project, err := createProject(token, name)
			if err != nil {
				return err
			}
			ui.Success("Created project %q (slug: %s)", project.Name, project.Slug)
			ui.Info("Run `seedmancer project use %s` to make it the default.", project.Slug)
			return nil
		},
	}
}

// listProjects calls GET /v1.0/projects and returns the list.
func listProjects(token string) ([]projectAPI, error) {
	baseURL := utils.GetBaseURL()
	reqURL := fmt.Sprintf("%s/v1.0/projects", baseURL)

	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %v", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var pr projectListResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("parsing response: %v", err)
	}
	return pr.Projects, nil
}

// createProject calls POST /v1.0/projects and returns the new project.
func createProject(token, name string) (projectAPI, error) {
	baseURL := utils.GetBaseURL()
	reqURL := fmt.Sprintf("%s/v1.0/projects", baseURL)

	payload, _ := json.Marshal(map[string]string{"name": name})
	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(payload))
	if err != nil {
		return projectAPI{}, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return projectAPI{}, fmt.Errorf("making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return projectAPI{}, fmt.Errorf("reading response: %v", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return projectAPI{}, utils.ErrInvalidAPIToken
	}
	if resp.StatusCode == http.StatusPaymentRequired || resp.StatusCode == http.StatusConflict {
		return projectAPI{}, fmt.Errorf("cannot create project: %s", extractErrorMessage(body))
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return projectAPI{}, fmt.Errorf("API request failed: %s - %s", resp.Status, string(body))
	}

	var p projectAPI
	if err := json.Unmarshal(body, &p); err != nil {
		return projectAPI{}, fmt.Errorf("parsing response: %v", err)
	}
	return p, nil
}

// extractErrorMessage pulls the first error message from a JSON error body.
// Falls back to the raw body string if parsing fails.
func extractErrorMessage(body []byte) string {
	var envelope struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err == nil && len(envelope.Errors) > 0 {
		return envelope.Errors[0].Message
	}
	return string(body)
}
