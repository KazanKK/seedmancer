package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KazanKK/seedmancer/cmd"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerResources exposes read-only views of the project state. MCP
// clients use these to build UI without having to call a tool first —
// e.g. Cursor renders `seedmancer://status` in a sidebar so the agent
// knows up-front which envs are configured.
func registerResources(s *mcp.Server) {
	// Project config (seedmancer.yaml verbatim).
	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://config",
		Name:        "seedmancer.yaml",
		Title:       "Project config",
		Description: "Raw contents of the project's seedmancer.yaml.",
		MIMEType:    "application/yaml",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		path, data, err := cmd.RawConfigBytes()
		if err != nil {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI:      req.Params.URI,
				MIMEType: "application/yaml",
				Text:     string(data),
				Meta:     mcp.Meta{"sourcePath": path},
			}},
		}, nil
	})

	// Structured status (same JSON shape as `seedmancer status --json`).
	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://status",
		Name:        "Seedmancer status",
		Title:       "Status (JSON)",
		Description: "Project layout, environments, and auth state as JSON.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		report, err := cmd.RunStatus(ctx, cmd.StatusInput{Offline: true})
		if err != nil {
			return nil, err
		}
		body, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
			}},
		}, nil
	})

	// Full dataset index (local + remote).
	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://datasets",
		Name:        "Datasets",
		Title:       "Datasets (local + remote)",
		Description: "JSON list of every dataset Seedmancer can see locally and in the cloud.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		out, err := cmd.RunList(ctx, cmd.ListInput{})
		if err != nil {
			return nil, err
		}
		body, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
			}},
		}, nil
	})

	// Local schemas (compact rows).
	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://schemas",
		Name:        "Schemas",
		Title:       "Local schemas",
		Description: "Compact list of schemas tracked in the local storage directory.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		rows, err := cmd.ListLocalSchemasBrief()
		if err != nil {
			return nil, err
		}
		body, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
			}},
		}, nil
	})

	// Dynamic per-dataset view. `{id}` is the dataset id (same name used
	// at seed/export time).
	s.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "seedmancer://dataset/{id}",
		Name:        "Dataset details",
		Title:       "Dataset details (JSON)",
		Description: "Structured details for one local dataset: files, row counts, CSV preview.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		id := strings.TrimPrefix(req.Params.URI, "seedmancer://dataset/")
		if id == "" || strings.Contains(id, "/") {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		out, err := cmd.RunDescribeDataset(ctx, cmd.DescribeDatasetInput{DatasetID: id})
		if err != nil {
			return nil, fmt.Errorf("dataset %q: %w", id, err)
		}
		body, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, err
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "application/json", Text: string(body),
			}},
		}, nil
	})

	// Static docs baked into the binary so agents can discover the
	// workflow without a network round-trip.
	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://docs/quickstart",
		Name:        "Quickstart",
		Title:       "Seedmancer quickstart",
		Description: "Short agent-oriented primer on how to use Seedmancer's tools together.",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "text/markdown", Text: docQuickstart,
			}},
		}, nil
	})

	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://docs/playwright-recipe",
		Name:        "Playwright recipe",
		Title:       "Reset DB before Playwright",
		Description: "Recipe for wiring seedmancer seed into a Playwright globalSetup.",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "text/markdown", Text: docPlaywrightRecipe,
			}},
		}, nil
	})

	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://docs/local-generation",
		Name:        "Local generation",
		Title:       "Generate datasets locally (no cloud)",
		Description: "How to use generate_dataset_local: Go script contract, examples, and common pitfalls.",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "text/markdown", Text: docLocalGeneration,
			}},
		}, nil
	})

	s.AddResource(&mcp.Resource{
		URI:         "seedmancer://docs/supabase-auth-connector",
		Name:        "Supabase Auth connector",
		Title:       "Supabase Auth service connector (Pro)",
		Description: "How to configure, export, and seed Supabase Auth users alongside Postgres using the Supabase Auth service connector.",
		MIMEType:    "text/markdown",
	}, func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{{
				URI: req.Params.URI, MIMEType: "text/markdown", Text: docSupabaseAuthConnector,
			}},
		}, nil
	})
}
