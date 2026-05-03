package services

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ─── realAIInferStripeMapping: happy path + failure modes ─────────────────────

func TestRealAIInferStripeMapping_HappyPath(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Account.csv"),
		[]byte("email,billing_customer_id\nuser@example.com,\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	wantRule := externalIDResolution{
		Table:        "Account",
		MatchColumn:  "email",
		OutputColumn: "billing_customer_id",
		ObjectAlias:  "customer:{email}",
	}
	wantCustomer := stripeCustomerObjectSpec{
		Alias:    "customer:{email}",
		Source:   stripeSourceSpec{Table: "Account", MatchColumn: "email"},
		Metadata: map[string]string{"seedmancer_managed": "true"},
	}

	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/v1.0/services/stripe/infer-mapping" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		resp := inferMappingResponse{
			ExternalIDResolution: []externalIDResolution{wantRule},
		}
		resp.Objects.Customers = []stripeCustomerObjectSpec{wantCustomer}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	rules, specs, err := realAIInferStripeMapping(
		context.Background(), server.URL, "tok-abc", dir, stripeSnapshot{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rules) != 1 || rules[0] != wantRule {
		t.Fatalf("unexpected rules: %+v", rules)
	}
	if len(specs.Customers) != 1 || specs.Customers[0].Alias != wantCustomer.Alias {
		t.Fatalf("unexpected customers: %+v", specs.Customers)
	}
	if !strings.HasPrefix(receivedAuth, "Bearer ") {
		t.Fatalf("expected Bearer auth header, got %q", receivedAuth)
	}
}

func TestRealAIInferStripeMapping_EntitlementFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Account.csv"),
		[]byte("email\nuser@example.com\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"errors":[{"code":"SERVICE_CONNECTORS_LIMIT","message":"Pro required"}]}`))
	}))
	defer server.Close()

	_, _, err := realAIInferStripeMapping(
		context.Background(), server.URL, "tok-abc", dir, stripeSnapshot{},
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "402") {
		t.Fatalf("expected error to mention 402, got: %v", err)
	}
}

func TestRealAIInferStripeMapping_ParseFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Account.csv"),
		[]byte("email\nuser@example.com\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json"))
	}))
	defer server.Close()

	_, _, err := realAIInferStripeMapping(
		context.Background(), server.URL, "tok-abc", dir, stripeSnapshot{},
	)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestRealAIInferStripeMapping_MissingTokenIsSoftSkip(t *testing.T) {
	rules, specs, err := realAIInferStripeMapping(
		context.Background(), "https://example.com", "", t.TempDir(), stripeSnapshot{},
	)
	if err != nil {
		t.Fatalf("expected nil error on missing token, got: %v", err)
	}
	if rules != nil || len(specs.Customers) > 0 || len(specs.Subscriptions) > 0 {
		t.Fatalf("expected empty result on missing token")
	}
}

func TestRealAIInferStripeMapping_NoCSVsIsSoftSkip(t *testing.T) {
	// No httptest server — function must short-circuit before any HTTP call.
	rules, specs, err := realAIInferStripeMapping(
		context.Background(), "http://127.0.0.1:1", "tok-abc", t.TempDir(), stripeSnapshot{},
	)
	if err != nil {
		t.Fatalf("expected nil error on empty data dir, got: %v", err)
	}
	if rules != nil || len(specs.Customers) > 0 || len(specs.Subscriptions) > 0 {
		t.Fatalf("expected empty result on empty data dir")
	}
}

// ─── Priority order: manual > AI > heuristic ──────────────────────────────────

func TestMergeResolutionRules_FirstSourceWins(t *testing.T) {
	manual := []externalIDResolution{
		{Table: "Account", MatchColumn: "email", OutputColumn: "billing_customer_id", ObjectAlias: "customer:{email}"},
	}
	ai := []externalIDResolution{
		// Same key as manual — must be skipped.
		{Table: "Account", MatchColumn: "email", OutputColumn: "billing_customer_id", ObjectAlias: "customer:{email}"},
		// Unique to AI — must be kept.
		{Table: "Account", MatchColumn: "email", OutputColumn: "billing_subscription_id", ObjectAlias: "subscription:{email}:pro_monthly"},
	}
	heuristic := []externalIDResolution{
		// Unique to heuristic — must be kept (last).
		{Table: "Org", MatchColumn: "id", OutputColumn: "stripe_customer_id", ObjectAlias: "customer:{id}"},
	}

	got := mergeResolutionRules(manual, ai, heuristic)
	if len(got) != 3 {
		t.Fatalf("expected 3 rules, got %d: %+v", len(got), got)
	}
	if got[0] != manual[0] {
		t.Fatalf("manual rule must come first, got %+v", got[0])
	}
	if got[1] != ai[1] {
		t.Fatalf("ai unique rule must come second, got %+v", got[1])
	}
	if got[2] != heuristic[0] {
		t.Fatalf("heuristic rule must come last, got %+v", got[2])
	}
}

func TestMergeInferred_FirstSourceWins(t *testing.T) {
	ai := inferredSpecs{
		Customers: []stripeCustomerObjectSpec{
			{Alias: "customer:{email}", Source: stripeSourceSpec{Table: "Account", MatchColumn: "email"}, Metadata: map[string]string{"by": "ai"}},
		},
	}
	heuristic := inferredSpecs{
		Customers: []stripeCustomerObjectSpec{
			// Same alias as AI — heuristic version must be skipped.
			{Alias: "customer:{email}", Source: stripeSourceSpec{Table: "Account", MatchColumn: "email"}, Metadata: map[string]string{"by": "heuristic"}},
		},
		Subscriptions: []stripeSubscriptionObjectSpec{
			{Alias: "subscription:{email}:pro_monthly", CustomerAlias: "customer:{email}", PriceKey: "pro_monthly"},
		},
	}

	got := mergeInferred(ai, heuristic)
	if len(got.Customers) != 1 {
		t.Fatalf("expected 1 customer, got %d", len(got.Customers))
	}
	if got.Customers[0].Metadata["by"] != "ai" {
		t.Fatalf("AI customer should win, got metadata: %+v", got.Customers[0].Metadata)
	}
	if len(got.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(got.Subscriptions))
	}
}

// ─── InferStripeMapping: produces a diff against the existing sidecar ─────────

func TestInferStripeMapping_ReturnsAddedAndRemovedDiff(t *testing.T) {
	prevRule := externalIDResolution{
		Table: "Account", MatchColumn: "email",
		OutputColumn: "old_column", ObjectAlias: "customer:{email}",
	}
	newRule := externalIDResolution{
		Table: "Account", MatchColumn: "email",
		OutputColumn: "billing_customer_id", ObjectAlias: "customer:{email}",
	}

	// Inject a deterministic AI response — no real network call.
	prev := aiInferStripeMapping
	t.Cleanup(func() { aiInferStripeMapping = prev })
	aiInferStripeMapping = func(_ context.Context, _, _, _ string, _ stripeSnapshot) ([]externalIDResolution, inferredSpecs, error) {
		return []externalIDResolution{newRule}, inferredSpecs{}, nil
	}

	snap := stripeSnapshot{
		Version:              stripeSnapshotVersion,
		Service:              "stripe",
		ExternalIDResolution: []externalIDResolution{prevRule},
	}
	snap.withDefaults()
	bytes, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	ctx := WithAICredentials(context.Background(), AICredentials{BaseURL: "http://example", Token: "tok"})
	got, err := InferStripeMapping(ctx, t.TempDir(), bytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.ExternalIDResolution) != 1 || got.ExternalIDResolution[0].OutputColumn != "billing_customer_id" {
		t.Fatalf("expected 1 proposed rule, got %+v", got.ExternalIDResolution)
	}
	if len(got.Added) != 1 || got.Added[0].OutputColumn != "billing_customer_id" {
		t.Fatalf("expected new rule in Added, got %+v", got.Added)
	}
	if len(got.Removed) != 1 || got.Removed[0].OutputColumn != "old_column" {
		t.Fatalf("expected old rule in Removed, got %+v", got.Removed)
	}
}

func TestInferStripeMapping_RequiresAICredentials(t *testing.T) {
	snap := stripeSnapshot{Version: stripeSnapshotVersion, Service: "stripe"}
	snap.withDefaults()
	bytes, _ := json.Marshal(snap)

	if _, err := InferStripeMapping(context.Background(), t.TempDir(), bytes); err == nil {
		t.Fatalf("expected error when AI credentials are missing")
	}
}
