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

	"github.com/KazanKK/seedmancer/internal/utils"
)

func TestNewStripeRejectsLiveKeyByDefault(t *testing.T) {
	t.Setenv("STRIPE_SECRET_KEY", "sk_live_123")
	_, err := newStripe("stripe", utils.ServiceConfig{Type: "stripe"})
	if err == nil {
		t.Fatal("expected live key to be rejected")
	}
	if !strings.Contains(err.Error(), "refusing to use live Stripe key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStripeListProductsActiveOnlyIgnoresArchived(t *testing.T) {
	var sawActiveFilter bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/products" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		sawActiveFilter = r.URL.Query().Get("active") == "true"
		writeJSON(t, w, map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "prod_active", "name": "Active", "active": true},
				{"id": "prod_archived", "name": "Archived", "active": false},
			},
		})
	}))
	defer server.Close()

	connector := testStripeConnector(server.URL)
	products, err := connector.listProducts(context.Background(), true, false)
	if err != nil {
		t.Fatalf("listProducts: %v", err)
	}
	if !sawActiveFilter {
		t.Fatal("expected active=true query")
	}
	if len(products) != 1 || products[0].ID != "prod_active" {
		t.Fatalf("expected only active product, got %#v", products)
	}
}

func TestStripeListPricesActiveOnlyIgnoresArchived(t *testing.T) {
	var sawActiveFilter bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/prices" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		sawActiveFilter = r.URL.Query().Get("active") == "true"
		writeJSON(t, w, map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "price_active", "lookup_key": "pro_monthly", "active": true},
				{"id": "price_archived", "lookup_key": "pro_monthly", "active": false},
			},
		})
	}))
	defer server.Close()

	connector := testStripeConnector(server.URL)
	prices, err := connector.listPrices(context.Background(), true, false)
	if err != nil {
		t.Fatalf("listPrices: %v", err)
	}
	if !sawActiveFilter {
		t.Fatal("expected active=true query")
	}
	if len(prices) != 1 || prices[0].ID != "price_active" {
		t.Fatalf("expected only active price, got %#v", prices)
	}
}

func TestStripeReconcilePriceImmutableFieldChangedArchivesAndCreates(t *testing.T) {
	var archivedOld bool
	var createdNew bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":          "price_old",
						"lookup_key":  "pro_monthly",
						"product":     "prod_1",
						"currency":    "usd",
						"unit_amount": 1000,
						"active":      true,
						"recurring": map[string]interface{}{
							"interval": "month",
						},
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/prices/price_old":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			archivedOld = r.Form.Get("active") == "false"
			writeJSON(t, w, map[string]interface{}{"id": "price_old", "active": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/prices":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			createdNew = r.Form.Get("lookup_key") == "pro_monthly" && r.Form.Get("unit_amount") == "2000"
			writeJSON(t, w, map[string]interface{}{
				"id":          "price_new",
				"lookup_key":  "pro_monthly",
				"product":     "prod_1",
				"currency":    "usd",
				"unit_amount": 2000,
				"active":      true,
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	connector := testStripeConnector(server.URL)
	id, err := connector.reconcilePrice(context.Background(), "prod_1", stripePriceSpec{
		Key:        "pro_monthly",
		LookupKey:  "pro_monthly",
		Currency:   "usd",
		UnitAmount: 2000,
		Recurring:  &stripeRecurring{Interval: "month"},
	})
	if err != nil {
		t.Fatalf("reconcilePrice: %v", err)
	}
	if id != "price_new" {
		t.Fatalf("expected price_new, got %s", id)
	}
	if !archivedOld {
		t.Fatal("expected old active price to be archived")
	}
	if !createdNew {
		t.Fatal("expected new price to be created with updated immutable fields")
	}
}

func TestStripeSeedDeleteRecreateAndResolveGenericColumns(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "Account.csv")
	if err := os.WriteFile(csvPath, []byte("email,billing_customer_id,billing_subscription_id\nuser@example.com,cus_old,sub_old\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	var deletedManagedCustomer bool
	var createdCustomer bool
	var createdSubscription bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/products/prod_1":
			writeJSON(t, w, map[string]interface{}{"id": "prod_1", "name": "Pro", "active": true})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":          "price_1",
						"lookup_key":  "pro_monthly",
						"product":     "prod_1",
						"currency":    "usd",
						"unit_amount": 2900,
						"active":      true,
						"recurring": map[string]interface{}{
							"interval": "month",
						},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":       "cus_old",
						"email":    "user@example.com",
						"metadata": map[string]string{},
					},
				},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/customers/cus_old":
			deletedManagedCustomer = true
			writeJSON(t, w, map[string]interface{}{"id": "cus_old", "deleted": true})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/customers":
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		createdCustomer = r.Form.Get("email") == "user@example.com"
		writeJSON(t, w, map[string]interface{}{"id": "cus_new", "email": "user@example.com"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_methods":
		writeJSON(t, w, map[string]interface{}{"id": "pm_test_visa"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_methods/pm_test_visa/attach":
		writeJSON(t, w, map[string]interface{}{"id": "pm_test_visa", "customer": "cus_new"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/customers/cus_new":
		writeJSON(t, w, map[string]interface{}{"id": "cus_new"})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/subscriptions":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			createdSubscription = r.Form.Get("customer") == "cus_new" && r.Form.Get("items[0][price]") == "price_1"
			writeJSON(t, w, map[string]interface{}{"id": "sub_new", "customer": "cus_new", "status": "active"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	snap := stripeSnapshot{
		Version: stripeSnapshotVersion,
		Service: "stripe",
		Catalog: stripeCatalogSpec{
			Products: []stripeProductSpec{
				{
					Key:      "pro",
					StripeID: "prod_1",
					Name:     "Pro",
					Active:   true,
					Prices: []stripePriceSpec{
						{
							Key:        "pro_monthly",
							LookupKey:  "pro_monthly",
							Currency:   "usd",
							UnitAmount: 2900,
							Active:     true,
							Recurring:  &stripeRecurring{Interval: "month"},
						},
					},
				},
			},
		},
		Customers: []stripeCustomerSnap{
			{Email: "user@example.com", Alias: "customer:{email}"},
		},
		Subscriptions: []stripeSubscriptionSnap{
			{
				CustomerEmail: "user@example.com",
				CustomerAlias: "customer:{email}",
				PriceKey:      "pro_monthly",
				Alias:         "subscription:{email}:pro_monthly",
			},
		},
		ExternalIDResolution: []externalIDResolution{
			{Table: "Account", MatchColumn: "email", OutputColumn: "billing_customer_id", ObjectAlias: "customer:{email}"},
			{Table: "Account", MatchColumn: "email", OutputColumn: "billing_subscription_id", ObjectAlias: "subscription:{email}:pro_monthly"},
		},
	}
	snap.withDefaults()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	connector := testStripeConnector(server.URL)
	if err := connector.Seed(WithDataDir(context.Background(), dir), data); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if !deletedManagedCustomer || !createdCustomer || !createdSubscription {
		t.Fatalf("expected delete/create flow, got delete=%v customer=%v subscription=%v", deletedManagedCustomer, createdCustomer, createdSubscription)
	}
	got, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read patched csv: %v", err)
	}
	if !strings.Contains(string(got), "user@example.com,cus_new,sub_new") {
		t.Fatalf("CSV was not patched with resolved external IDs:\n%s", string(got))
	}
}

// TestStripeExport_PriorityOrder_ManualBeatsAIBeatsHeuristic verifies the
// Export flow merges externalIdResolution rules in the priority order
// promised in the plan: existing/manual > AI > heuristic. We inject a
// deterministic AI function so the test does not need a real backend, and
// stand up a minimal Stripe httptest server that returns one product, one
// price, and one matching customer/subscription.
func TestStripeExport_PriorityOrder_ManualBeatsAIBeatsHeuristic(t *testing.T) {
	dir := t.TempDir()
	// CSV has a column named billing_customer_id — the heuristic + AI both
	// see this column and propose mappings for it. The existing sidecar
	// already has a manual rule pointing to a different (legacy) column —
	// that manual rule must survive untouched.
	if err := os.WriteFile(
		filepath.Join(dir, "Account.csv"),
		[]byte("email,billing_customer_id,legacy_customer_col\nuser@example.com,cus_old,cus_legacy\n"),
		0644,
	); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/products":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{"id": "prod_1", "name": "Pro", "active": true},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id": "price_1", "lookup_key": "pro_monthly", "product": "prod_1",
						"currency": "usd", "unit_amount": 2900, "active": true,
						"recurring": map[string]interface{}{"interval": "month"},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":       "cus_existing",
						"email":    "user@example.com",
						"metadata": map[string]string{},
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/subscriptions":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	manualRule := externalIDResolution{
		Table:        "Account",
		MatchColumn:  "email",
		OutputColumn: "legacy_customer_col",
		ObjectAlias:  "customer:{email}",
	}
	aiOnlyRule := externalIDResolution{
		Table:        "Account",
		MatchColumn:  "email",
		OutputColumn: "ai_proposed_col",
		ObjectAlias:  "customer:{email}",
	}

	prev := aiInferStripeMapping
	t.Cleanup(func() { aiInferStripeMapping = prev })
	aiCalled := false
	aiInferStripeMapping = func(_ context.Context, _, _, _ string, _ stripeSnapshot) ([]externalIDResolution, inferredSpecs, error) {
		aiCalled = true
		return []externalIDResolution{
			// Conflicts with manual on (Account, email, legacy_customer_col, customer:{email}).
			// AI version must lose to the manual one.
			manualRule,
			// Net-new rule from AI — must appear before any heuristic rule.
			aiOnlyRule,
		}, inferredSpecs{}, nil
	}

	// Seed the existing sidecar via t.TempDir so we can write _stripe.json
	// with the manual rule pre-populated.
	connector := testStripeConnector(server.URL)
	manualSnap := stripeSnapshot{
		Version:              stripeSnapshotVersion,
		Service:              "stripe",
		ExternalIDResolution: []externalIDResolution{manualRule},
	}
	manualSnap.withDefaults()
	manualBytes, err := json.Marshal(manualSnap)
	if err != nil {
		t.Fatalf("marshal manual snap: %v", err)
	}
	sidecarPath := filepath.Join(dir, SidecarFilename(connector.name))
	if err := os.WriteFile(sidecarPath, manualBytes, 0644); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}

	ctx := WithDataDir(context.Background(), dir)
	ctx = WithAICredentials(ctx, AICredentials{BaseURL: "http://example", Token: "tok"})
	out, err := connector.Export(ctx)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	var got stripeSnapshot
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal export: %v", err)
	}
	if !aiCalled {
		t.Fatal("expected AI inference to be invoked")
	}
	// Manual rule must come first in the merged list.
	if len(got.ExternalIDResolution) == 0 || got.ExternalIDResolution[0] != manualRule {
		t.Fatalf("expected manual rule to come first, got: %+v", got.ExternalIDResolution)
	}
	// AI-only rule must appear (heuristic-only column from CSV like
	// billing_customer_id is also expected). What matters for priority is:
	// AI rules appear before heuristic rules. Find positions:
	pos := func(rule externalIDResolution) int {
		for i, r := range got.ExternalIDResolution {
			if r == rule {
				return i
			}
		}
		return -1
	}
	aiPos := pos(aiOnlyRule)
	if aiPos < 0 {
		t.Fatalf("expected AI-only rule in merged list, got: %+v", got.ExternalIDResolution)
	}
	// The heuristic column is `billing_customer_id`. It must come AFTER the
	// AI-only rule.
	heuristicPos := -1
	for i, r := range got.ExternalIDResolution {
		if r.OutputColumn == "billing_customer_id" {
			heuristicPos = i
			break
		}
	}
	if heuristicPos >= 0 && heuristicPos < aiPos {
		t.Fatalf("heuristic rule must come after AI rule, got positions ai=%d heuristic=%d", aiPos, heuristicPos)
	}
}

// TestStripeExport_NoAIInfer_SkipsAICall verifies that WithNoAIInfer(ctx)
// suppresses the AI call entirely.
func TestStripeExport_NoAIInfer_SkipsAICall(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, "Account.csv"),
		[]byte("email\nuser@example.com\n"),
		0644,
	); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/products":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/subscriptions":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	prev := aiInferStripeMapping
	t.Cleanup(func() { aiInferStripeMapping = prev })
	aiCalled := false
	aiInferStripeMapping = func(_ context.Context, _, _, _ string, _ stripeSnapshot) ([]externalIDResolution, inferredSpecs, error) {
		aiCalled = true
		return nil, inferredSpecs{}, nil
	}

	connector := testStripeConnector(server.URL)
	ctx := WithDataDir(context.Background(), dir)
	ctx = WithAICredentials(ctx, AICredentials{BaseURL: "http://example", Token: "tok"})
	ctx = WithNoAIInfer(ctx)
	if _, err := connector.Export(ctx); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if aiCalled {
		t.Fatal("expected AI inference to be skipped when WithNoAIInfer is set")
	}
}

func TestStripeSeedTrialingSubscriptionSendsTrialPeriodDays(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "Account.csv")
	if err := os.WriteFile(csvPath, []byte("email,billing_customer_id,billing_subscription_id\nuser@example.com,,\n"), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	var trialPeriodDays string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/products/prod_1":
			writeJSON(t, w, map[string]interface{}{"id": "prod_1", "name": "Pro", "active": true})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
			writeJSON(t, w, map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id": "price_1", "lookup_key": "pro_monthly", "product": "prod_1",
						"currency": "usd", "unit_amount": 2900, "active": true,
						"recurring": map[string]interface{}{"interval": "month"},
					},
				},
				"has_more": false,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/customers":
			writeJSON(t, w, map[string]interface{}{"data": []map[string]interface{}{}, "has_more": false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/customers":
			writeJSON(t, w, map[string]interface{}{"id": "cus_new", "email": "user@example.com"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_methods":
			writeJSON(t, w, map[string]interface{}{"id": "pm_test_visa"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/payment_methods/pm_test_visa/attach":
			writeJSON(t, w, map[string]interface{}{"id": "pm_test_visa", "customer": "cus_new"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/customers/cus_new":
			writeJSON(t, w, map[string]interface{}{"id": "cus_new"})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/subscriptions":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("parse form: %v", err)
			}
			trialPeriodDays = r.Form.Get("trial_period_days")
			writeJSON(t, w, map[string]interface{}{"id": "sub_new", "customer": "cus_new", "status": "trialing"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	snap := stripeSnapshot{
		Version: stripeSnapshotVersion,
		Service: "stripe",
		Catalog: stripeCatalogSpec{
			Products: []stripeProductSpec{
				{
					Key: "pro", StripeID: "prod_1", Name: "Pro", Active: true,
					Prices: []stripePriceSpec{
						{
							Key: "pro_monthly", LookupKey: "pro_monthly",
							Currency: "usd", UnitAmount: 2900, Active: true,
							Recurring: &stripeRecurring{Interval: "month"},
						},
					},
				},
			},
		},
		Customers: []stripeCustomerSnap{
			{Email: "user@example.com", Alias: "customer:{email}"},
		},
		Subscriptions: []stripeSubscriptionSnap{
			{
				CustomerEmail:   "user@example.com",
				CustomerAlias:   "customer:{email}",
				PriceKey:        "pro_monthly",
				Alias:           "subscription:{email}:pro_monthly",
				Status:          "trialing",
				TrialPeriodDays: 14,
			},
		},
		ExternalIDResolution: []externalIDResolution{
			{Table: "Account", MatchColumn: "email", OutputColumn: "billing_customer_id", ObjectAlias: "customer:{email}"},
			{Table: "Account", MatchColumn: "email", OutputColumn: "billing_subscription_id", ObjectAlias: "subscription:{email}:pro_monthly"},
		},
	}
	snap.withDefaults()
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	connector := testStripeConnector(server.URL)
	if err := connector.Seed(WithDataDir(context.Background(), dir), data); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	if trialPeriodDays != "14" {
		t.Fatalf("expected trial_period_days=14 for trialing subscription, got %q", trialPeriodDays)
	}
}

func testStripeConnector(apiBase string) *stripeConnector {
	return &stripeConnector{
		name:       "stripe",
		apiKey:     "sk_test_123",
		apiBase:    apiBase,
		httpClient: http.DefaultClient,
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, body interface{}) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Fatalf("encode json: %v", err)
	}
}
