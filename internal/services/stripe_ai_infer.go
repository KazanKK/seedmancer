package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	utils "github.com/KazanKK/seedmancer/internal/utils"
)

// aiInferStripeMappingFunc is the function type used to call the Seedmancer
// cloud's AI mapping inference endpoint. Stored as a package variable so
// tests can inject deterministic behavior without touching the network.
type aiInferStripeMappingFunc func(
	ctx context.Context,
	baseURL, token, dataDir string,
	snap stripeSnapshot,
) (rules []externalIDResolution, specs inferredSpecs, err error)

// aiInferStripeMapping is the production implementation. Tests overwrite this
// variable to inject canned responses (and restore it via t.Cleanup).
var aiInferStripeMapping aiInferStripeMappingFunc = realAIInferStripeMapping

// inferMappingMaxSampleRows caps how many rows per CSV are sent to the model.
// The prompt only needs representative values, not full content.
const inferMappingMaxSampleRows = 5

// inferMappingMaxCellLen is the maximum byte length of a single CSV cell
// value sent to the backend. The route's Zod schema enforces .max(500), so
// we truncate client-side with a small safety margin.
const inferMappingMaxCellLen = 490

// inferMappingHTTPTimeout bounds how long the export waits on the AI call.
// AI inference is best-effort; we never want to block a working export on a
// slow model response.
var inferMappingHTTPTimeout = 30 * time.Second

// inferMappingHTTPClient is overridable for tests.
var inferMappingHTTPClient = &http.Client{Timeout: inferMappingHTTPTimeout}

// inferMappingRequest is the wire shape for POST /v1.0/services/stripe/infer-mapping.
type inferMappingRequest struct {
	CSVs     []inferMappingCSV     `json:"csvs"`
	Snapshot inferMappingSnapshot `json:"snapshot"`
}

type inferMappingCSV struct {
	Table      string     `json:"table"`
	Header     []string   `json:"header"`
	SampleRows [][]string `json:"sampleRows"`
}

type inferMappingSnapshot struct {
	Customers     []inferMappingCustomer     `json:"customers"`
	Subscriptions []inferMappingSubscription `json:"subscriptions"`
	Catalog       inferMappingCatalog        `json:"catalog"`
}

type inferMappingCustomer struct {
	Email    string `json:"email,omitempty"`
	StripeID string `json:"stripeId,omitempty"`
}

type inferMappingSubscription struct {
	StripeID      string `json:"stripeId,omitempty"`
	Customer      string `json:"customer,omitempty"`
	CustomerEmail string `json:"customerEmail,omitempty"`
	PriceKey      string `json:"priceKey,omitempty"`
	Status        string `json:"status,omitempty"`
}

type inferMappingCatalog struct {
	Products []inferMappingProduct `json:"products"`
}

type inferMappingProduct struct {
	Key    string              `json:"key"`
	Name   string              `json:"name"`
	Prices []inferMappingPrice `json:"prices"`
}

type inferMappingPrice struct {
	Key        string `json:"key"`
	Currency   string `json:"currency,omitempty"`
	UnitAmount int64  `json:"unitAmount,omitempty"`
}

// inferMappingResponse mirrors the route's return shape.
type inferMappingResponse struct {
	ExternalIDResolution []externalIDResolution `json:"externalIdResolution"`
	Objects              struct {
		Customers     []stripeCustomerObjectSpec     `json:"customers"`
		Subscriptions []stripeSubscriptionObjectSpec `json:"subscriptions"`
	} `json:"objects"`
}

// realAIInferStripeMapping calls POST /v1.0/services/stripe/infer-mapping.
//
// Returns nil rules / empty specs and a nil error when the call cannot be
// made (no token, no CSVs, no snapshot data). Network and 4xx/5xx errors
// are surfaced so the caller can decide whether to log and continue.
func realAIInferStripeMapping(
	ctx context.Context,
	baseURL, token, dataDir string,
	snap stripeSnapshot,
) ([]externalIDResolution, inferredSpecs, error) {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(baseURL) == "" {
		return nil, inferredSpecs{}, nil
	}

	csvs, err := collectCSVSummaries(dataDir)
	if err != nil {
		return nil, inferredSpecs{}, fmt.Errorf("collect csvs: %w", err)
	}
	if len(csvs) == 0 {
		return nil, inferredSpecs{}, nil
	}

	body := inferMappingRequest{
		CSVs:     csvs,
		Snapshot: buildSnapshotPayload(snap),
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, inferredSpecs{}, fmt.Errorf("marshal request: %w", err)
	}

	url := strings.TrimRight(baseURL, "/") + "/v1.0/services/stripe/infer-mapping"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, inferredSpecs{}, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Authorization", utils.BearerAPIToken(token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := inferMappingHTTPClient.Do(req)
	if err != nil {
		return nil, inferredSpecs{}, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		// 402 (Pro plan), 401 (token), 502 (model failure) all land here.
		// Callers treat any non-OK as a soft skip + log.
		return nil, inferredSpecs{}, fmt.Errorf("infer-mapping: status %s: %s", resp.Status, truncateForLog(string(respBody), 300))
	}

	var parsed inferMappingResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, inferredSpecs{}, fmt.Errorf("decode response: %w", err)
	}

	specs := inferredSpecs{
		Customers:     parsed.Objects.Customers,
		Subscriptions: parsed.Objects.Subscriptions,
	}
	return parsed.ExternalIDResolution, specs, nil
}

// collectCSVSummaries reads every *.csv in dataDir and returns the header
// plus up to inferMappingMaxSampleRows sample rows for each. Cell values are
// truncated to inferMappingMaxCellLen characters so the request stays within
// the backend's Zod validation limits. Files that fail to parse are skipped
// silently — an unparseable CSV shouldn't block AI inference for the rest.
func collectCSVSummaries(dataDir string) ([]inferMappingCSV, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var out []inferMappingCSV
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		table := strings.TrimSuffix(entry.Name(), ".csv")
		header, records, err := readCSVFile(filepath.Join(dataDir, entry.Name()))
		if err != nil {
			continue
		}
		// Keep at most N sample rows, truncating long cell values.
		sampleN := inferMappingMaxSampleRows
		if len(records) < sampleN {
			sampleN = len(records)
		}
		truncated := make([][]string, sampleN)
		for i, row := range records[:sampleN] {
			truncated[i] = make([]string, len(row))
			for j, cell := range row {
				if len(cell) > inferMappingMaxCellLen {
					truncated[i][j] = cell[:inferMappingMaxCellLen]
				} else {
					truncated[i][j] = cell
				}
			}
		}
		out = append(out, inferMappingCSV{
			Table:      table,
			Header:     header,
			SampleRows: truncated,
		})
	}
	return out, nil
}

// buildSnapshotPayload extracts only the fields the model needs from a
// stripeSnapshot so we never leak unrelated metadata to the LLM.
func buildSnapshotPayload(snap stripeSnapshot) inferMappingSnapshot {
	out := inferMappingSnapshot{
		Catalog: inferMappingCatalog{Products: []inferMappingProduct{}},
	}
	for _, c := range snap.Customers {
		out.Customers = append(out.Customers, inferMappingCustomer{
			Email:    c.Email,
			StripeID: c.StripeID,
		})
	}
	for _, s := range snap.Subscriptions {
		out.Subscriptions = append(out.Subscriptions, inferMappingSubscription{
			StripeID:      s.StripeID,
			Customer:      s.Customer,
			CustomerEmail: s.CustomerEmail,
			PriceKey:      s.PriceKey,
			Status:        s.Status,
		})
	}
	for _, p := range snap.Catalog.Products {
		prod := inferMappingProduct{Key: p.Key, Name: p.Name}
		for _, pr := range p.Prices {
			prod.Prices = append(prod.Prices, inferMappingPrice{
				Key:        pr.Key,
				Currency:   pr.Currency,
				UnitAmount: pr.UnitAmount,
			})
		}
		out.Catalog.Products = append(out.Catalog.Products, prod)
	}
	return out
}

func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// InferStripeMappingResult is the structured proposal returned by
// InferStripeMapping. Exported types so MCP / cmd can serialize the result.
type InferStripeMappingResult struct {
	// Proposed rules computed from AI inference.
	ExternalIDResolution []InferStripeMappingRule `json:"externalIdResolution"`
	// Proposed customer/subscription specs needed by the seeder.
	Objects InferStripeMappingObjects `json:"objects"`
	// Added rules (in proposal but not in current sidecar).
	Added []InferStripeMappingRule `json:"added"`
	// Removed rules (in current sidecar but not in proposal). May be ignored
	// by callers that prefer additive merging — useful for visibility only.
	Removed []InferStripeMappingRule `json:"removed"`
}

// InferStripeMappingRule mirrors externalIDResolution as a public type.
type InferStripeMappingRule struct {
	Table        string `json:"table"`
	MatchColumn  string `json:"matchColumn"`
	OutputColumn string `json:"outputColumn"`
	ObjectAlias  string `json:"objectAlias"`
}

// InferStripeMappingObjects mirrors stripeObjectSpecs as a public type.
type InferStripeMappingObjects struct {
	Customers     []InferStripeMappingCustomer     `json:"customers"`
	Subscriptions []InferStripeMappingSubscription `json:"subscriptions"`
}

type InferStripeMappingCustomer struct {
	Alias    string            `json:"alias"`
	Source   InferMappingSpec  `json:"source"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type InferStripeMappingSubscription struct {
	Alias         string           `json:"alias"`
	CustomerAlias string           `json:"customerAlias"`
	PriceKey      string           `json:"priceKey"`
	Source        InferMappingSpec `json:"source"`
}

type InferMappingSpec struct {
	Table       string `json:"table"`
	MatchColumn string `json:"matchColumn"`
}

// InferStripeMapping reads an existing Stripe sidecar, runs AI inference
// against the snapshot it contains plus the dataset CSVs, and returns the
// proposed rules + a diff against what's already in the sidecar. Does not
// modify the sidecar — callers (MCP tool, dry-run command) decide what to
// do with the proposal.
//
// AI credentials must be provided via WithAICredentials on ctx. When no
// token is configured this returns an error so callers can surface a clear
// "log in / set token" message.
func InferStripeMapping(ctx context.Context, dataDir string, sidecarBytes []byte) (InferStripeMappingResult, error) {
	creds, ok := AICredentialsFromContext(ctx)
	if !ok || strings.TrimSpace(creds.Token) == "" {
		return InferStripeMappingResult{}, fmt.Errorf("AI inference requires an API token (run `seedmancer login` or set SEEDMANCER_API_TOKEN)")
	}

	var snap stripeSnapshot
	if err := json.Unmarshal(sidecarBytes, &snap); err != nil {
		return InferStripeMappingResult{}, fmt.Errorf("parse sidecar: %w", err)
	}
	snap.withDefaults()

	rules, specs, err := aiInferStripeMapping(ctx, creds.BaseURL, creds.Token, dataDir, snap)
	if err != nil {
		return InferStripeMappingResult{}, err
	}

	currentKeys := map[string]bool{}
	for _, r := range snap.ExternalIDResolution {
		currentKeys[resolutionKey(r)] = true
	}
	proposedKeys := map[string]bool{}
	out := InferStripeMappingResult{}
	for _, r := range rules {
		out.ExternalIDResolution = append(out.ExternalIDResolution, toPublicRule(r))
		proposedKeys[resolutionKey(r)] = true
		if !currentKeys[resolutionKey(r)] {
			out.Added = append(out.Added, toPublicRule(r))
		}
	}
	for _, r := range snap.ExternalIDResolution {
		if !proposedKeys[resolutionKey(r)] {
			out.Removed = append(out.Removed, toPublicRule(r))
		}
	}
	for _, c := range specs.Customers {
		out.Objects.Customers = append(out.Objects.Customers, InferStripeMappingCustomer{
			Alias:    c.Alias,
			Source:   InferMappingSpec{Table: c.Source.Table, MatchColumn: c.Source.MatchColumn},
			Metadata: c.Metadata,
		})
	}
	for _, s := range specs.Subscriptions {
		out.Objects.Subscriptions = append(out.Objects.Subscriptions, InferStripeMappingSubscription{
			Alias:         s.Alias,
			CustomerAlias: s.CustomerAlias,
			PriceKey:      s.PriceKey,
			Source:        InferMappingSpec{Table: s.Source.Table, MatchColumn: s.Source.MatchColumn},
		})
	}
	return out, nil
}

func toPublicRule(r externalIDResolution) InferStripeMappingRule {
	return InferStripeMappingRule{
		Table:        r.Table,
		MatchColumn:  r.MatchColumn,
		OutputColumn: r.OutputColumn,
		ObjectAlias:  r.ObjectAlias,
	}
}
