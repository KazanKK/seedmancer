package services

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/utils"
)

const (
	stripeDefaultAPIBase       = "https://api.stripe.com"
	stripeDefaultManagedKey    = "seedmancer_managed"
	stripeDefaultManagedValue  = "true"
	stripeSnapshotVersion      = 1
	stripeDefaultCustomerAlias = "customer:{%s}"
)

type stripeConnector struct {
	name       string
	apiKey     string
	apiBase    string
	httpClient *http.Client
}

func newStripe(name string, cfg utils.ServiceConfig) (*stripeConnector, error) {
	keyRaw := strings.TrimSpace(cfg.APIKeyEnv)
	if keyRaw == "" {
		keyRaw = "STRIPE_SECRET_KEY"
	}
	apiKey, err := resolveValue(name, "apiKeyEnv", keyRaw)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(apiKey, "sk_live_") || strings.HasPrefix(apiKey, "rk_live_") {
		return nil, fmt.Errorf("service %q: refusing to use live Stripe key — Seedmancer only supports test-mode keys", name)
	}
	apiBase := strings.TrimRight(os.Getenv("SEEDMANCER_STRIPE_API_BASE"), "/")
	if apiBase == "" {
		apiBase = stripeDefaultAPIBase
	}
	return &stripeConnector{
		name:       name,
		apiKey:     apiKey,
		apiBase:    apiBase,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (c *stripeConnector) ServiceType() string { return "stripe" }

// Export imports Stripe state into the dataset sidecar. If an existing
// _stripe.json already carries resolution rules, those rules are preserved and
// refreshed with current Stripe IDs.
func (c *stripeConnector) Export(ctx context.Context) ([]byte, error) {
	dataDir, hasDataDir := DataDirFromContext(ctx)
	existing := stripeSnapshot{}
	if hasDataDir {
		data, err := os.ReadFile(filepath.Join(dataDir, SidecarFilename(c.name)))
		if err == nil {
			_ = json.Unmarshal(data, &existing)
		}
	}
	existing.withDefaults()

	products, err := c.listProducts(ctx, existing.Catalog.ActiveOnly, existing.Catalog.IncludeArchived)
	if err != nil {
		return nil, fmt.Errorf("stripe export: list products: %w", err)
	}
	prices, err := c.listPrices(ctx, existing.Catalog.ActiveOnly, existing.Catalog.IncludeArchived)
	if err != nil {
		return nil, fmt.Errorf("stripe export: list prices: %w", err)
	}
	// List all customers for inference + subscription email resolution.
	// Managed-only filtering applies at seed reset time, not here.
	allCustomers, err := c.listAllCustomers(ctx)
	if err != nil {
		return nil, fmt.Errorf("stripe export: list customers: %w", err)
	}
	managedCustomers := filterManaged(allCustomers, existing.Reset.Customers.ManagedMetadataKey, existing.Reset.Customers.ManagedMetadataValue)
	subscriptions, err := c.listSubscriptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("stripe export: list subscriptions: %w", err)
	}

	catalog := buildStripeCatalog(existing.Catalog, products, prices)

	// Build a price ID → catalog key map so subscriptionSnapshots uses the
	// same stable keys as the catalog (not raw Stripe IDs or lookup keys).
	catalogPriceKeys := make(map[string]string)
	for _, prod := range catalog.Products {
		for _, p := range prod.Prices {
			if p.StripeID != "" {
				catalogPriceKeys[p.StripeID] = p.Key
			}
		}
	}

	snap := stripeSnapshot{
		Version:              stripeSnapshotVersion,
		Service:              "stripe",
		CapturedAt:           time.Now().UTC(),
		Catalog:              catalog,
		Reset:                existing.Reset,
		Objects:              existing.Objects,
		ExternalIDResolution: existing.ExternalIDResolution,
		Customers:            customerSnapshots(managedCustomers),
		Subscriptions:        subscriptionSnapshots(subscriptions, allCustomers, catalogPriceKeys),
	}
	snap.withDefaults()
	if hasDataDir {
		heuristicRules, heuristicObjects := inferStripeResolution(dataDir, snap)

		// AI inference is best-effort: any failure (missing token, Free plan,
		// model error) is logged once and the export continues with whatever
		// the heuristic produced. Manual rules in the existing sidecar always
		// take precedence over both AI + heuristic via mergeResolutionRules's
		// first-source-wins semantics.
		var aiRules []externalIDResolution
		var aiObjects inferredSpecs
		if !NoAIInferFromContext(ctx) {
			creds, ok := AICredentialsFromContext(ctx)
			if ok && strings.TrimSpace(creds.Token) != "" {
				rules, objects, err := aiInferStripeMapping(ctx, creds.BaseURL, creds.Token, dataDir, snap)
				if err != nil {
					logAIInferWarning(c.name, err)
				} else {
					aiRules = rules
					aiObjects = objects
				}
			}
		}

		snap.ExternalIDResolution = mergeResolutionRules(
			snap.ExternalIDResolution,
			aiRules,
			heuristicRules,
		)
		snap.Objects = mergeObjectSpecs(snap.Objects, mergeInferred(aiObjects, heuristicObjects))
	}
	out, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("stripe export: marshal: %w", err)
	}
	return out, nil
}

// Seed resets managed Stripe test objects, recreates them from CSV rows, then
// resolves configured external IDs in the materialized CSV directory.
func (c *stripeConnector) Seed(ctx context.Context, snapshot []byte) error {
	var snap stripeSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return fmt.Errorf("stripe seed: unmarshal snapshot: %w", err)
	}
	if snap.Version != stripeSnapshotVersion {
		return fmt.Errorf("stripe seed: unsupported snapshot version %d", snap.Version)
	}
	snap.withDefaults()
	dataDir, ok := DataDirFromContext(ctx)
	if !ok {
		return fmt.Errorf("stripe seed: dataset directory is required for external ID resolution")
	}
	if len(snap.ExternalIDResolution) == 0 {
		return fmt.Errorf("stripe seed: _stripe.json has no externalIdResolution rules")
	}
	priceIDs, err := c.reconcileCatalog(ctx, snap.Catalog)
	if err != nil {
		return err
	}
	if err := c.resetManagedObjects(ctx, snap.Reset); err != nil {
		return err
	}
	objectIDs, rows, err := c.recreateCustomers(ctx, dataDir, snap)
	if err != nil {
		return err
	}
	if err := c.recreateSubscriptions(ctx, snap, priceIDs, objectIDs, rows); err != nil {
		return err
	}
	return resolveExternalIDs(dataDir, snap.ExternalIDResolution, objectIDs)
}

type stripeSnapshot struct {
	Version              int                      `json:"version"`
	Service              string                   `json:"service"`
	CapturedAt           time.Time                `json:"capturedAt"`
	Catalog              stripeCatalogSpec        `json:"catalog"`
	Reset                stripeResetSpec          `json:"reset"`
	Objects              stripeObjectSpecs        `json:"objects"`
	ExternalIDResolution []externalIDResolution   `json:"externalIdResolution,omitempty"`
	Customers            []stripeCustomerSnap     `json:"customers,omitempty"`
	Subscriptions        []stripeSubscriptionSnap `json:"subscriptions,omitempty"`
}

type stripeCatalogSpec struct {
	ActiveOnly      bool                `json:"activeOnly"`
	IncludeArchived bool                `json:"includeArchived"`
	Products        []stripeProductSpec `json:"products,omitempty"`
}

type stripeProductSpec struct {
	Key         string            `json:"key"`
	StripeID    string            `json:"stripeId,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Active      bool              `json:"active"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Prices      []stripePriceSpec `json:"prices,omitempty"`
}

type stripePriceSpec struct {
	Key        string           `json:"key"`
	StripeID   string           `json:"stripeId,omitempty"`
	LookupKey  string           `json:"lookupKey,omitempty"`
	Currency   string           `json:"currency"`
	UnitAmount int64            `json:"unitAmount"`
	Nickname   string           `json:"nickname,omitempty"`
	Active     bool             `json:"active"`
	Recurring  *stripeRecurring `json:"recurring,omitempty"`
}

type stripeRecurring struct {
	Interval      string `json:"interval"`
	IntervalCount int64  `json:"intervalCount,omitempty"`
}

type stripeResetSpec struct {
	Customers stripeCustomerResetSpec `json:"customers"`
}

type stripeCustomerResetSpec struct {
	Mode                 string `json:"mode"`
	ManagedMetadataKey   string `json:"managedMetadataKey"`
	ManagedMetadataValue string `json:"managedMetadataValue"`
}

type stripeObjectSpecs struct {
	Customers     []stripeCustomerObjectSpec     `json:"customers,omitempty"`
	Subscriptions []stripeSubscriptionObjectSpec `json:"subscriptions,omitempty"`
}

type stripeSourceSpec struct {
	Table       string `json:"table"`
	MatchColumn string `json:"matchColumn"`
}

type stripeCustomerObjectSpec struct {
	Alias    string            `json:"alias"`
	Source   stripeSourceSpec  `json:"source"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type stripeSubscriptionObjectSpec struct {
	Alias         string           `json:"alias"`
	CustomerAlias string           `json:"customerAlias"`
	PriceKey      string           `json:"priceKey"`
	Source        stripeSourceSpec `json:"source,omitempty"`
	// TrialPeriodDays creates the subscription in trialing status.
	// Takes precedence over PaymentBehavior when set.
	TrialPeriodDays int `json:"trialPeriodDays,omitempty"`
	// PaymentBehavior controls how Stripe handles a missing payment method.
	// Defaults to "default_incomplete" for test mode seeding so no payment
	// method is required. Set to "" to use Stripe's default.
	PaymentBehavior string `json:"paymentBehavior,omitempty"`
}

type externalIDResolution struct {
	Table        string `json:"table"`
	MatchColumn  string `json:"matchColumn"`
	OutputColumn string `json:"outputColumn"`
	ObjectAlias  string `json:"objectAlias"`
}

type stripeCustomerSnap struct {
	Email    string            `json:"email,omitempty"`
	StripeID string            `json:"stripeId,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type stripeSubscriptionSnap struct {
	StripeID      string `json:"stripeId,omitempty"`
	Customer      string `json:"customer,omitempty"`
	CustomerEmail string `json:"customerEmail,omitempty"`
	PriceKey      string `json:"priceKey,omitempty"`
	Status        string `json:"status,omitempty"`
}

func (s *stripeSnapshot) withDefaults() {
	if s.Version == 0 {
		s.Version = stripeSnapshotVersion
	}
	if s.Service == "" {
		s.Service = "stripe"
	}
	if !s.Catalog.IncludeArchived {
		s.Catalog.ActiveOnly = true
	}
	if s.Reset.Customers.Mode == "" {
		s.Reset.Customers.Mode = "delete_recreate"
	}
	if s.Reset.Customers.ManagedMetadataKey == "" {
		s.Reset.Customers.ManagedMetadataKey = stripeDefaultManagedKey
	}
	if s.Reset.Customers.ManagedMetadataValue == "" {
		s.Reset.Customers.ManagedMetadataValue = stripeDefaultManagedValue
	}
}

type stripeProduct struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Active      bool              `json:"active"`
	Metadata    map[string]string `json:"metadata"`
}

type stripePrice struct {
	ID         string           `json:"id"`
	Product    string           `json:"product"`
	LookupKey  string           `json:"lookup_key"`
	Currency   string           `json:"currency"`
	UnitAmount int64            `json:"unit_amount"`
	Nickname   string           `json:"nickname"`
	Active     bool             `json:"active"`
	Recurring  *stripeRecurring `json:"recurring"`
}

type stripeCustomer struct {
	ID       string            `json:"id"`
	Email    string            `json:"email"`
	Metadata map[string]string `json:"metadata"`
}

type stripeSubscription struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Status   string `json:"status"`
	Items    struct {
		Data []struct {
			Price stripePrice `json:"price"`
		} `json:"data"`
	} `json:"items"`
}

func (c *stripeConnector) reconcileCatalog(ctx context.Context, catalog stripeCatalogSpec) (map[string]string, error) {
	priceIDs := map[string]string{}
	for _, product := range catalog.Products {
		productID, err := c.reconcileProduct(ctx, product)
		if err != nil {
			return nil, err
		}
		for _, price := range product.Prices {
			id, err := c.reconcilePrice(ctx, productID, price)
			if err != nil {
				return nil, err
			}
			priceIDs[price.Key] = id
		// Also accept the original stripeId as an alias so that existing
		// _stripe.json files whose subscriptions reference raw Stripe price
		// IDs (instead of logical keys) continue to work after reconcile.
		if price.StripeID != "" {
			priceIDs[price.StripeID] = id
		}
		}
	}
	return priceIDs, nil
}

func (c *stripeConnector) reconcileProduct(ctx context.Context, spec stripeProductSpec) (string, error) {
	if spec.StripeID != "" {
		product, err := c.retrieveProduct(ctx, spec.StripeID)
		if err == nil && product.Active {
			return product.ID, nil
		}
	}
	products, err := c.listProducts(ctx, true, false)
	if err != nil {
		return "", fmt.Errorf("stripe seed: list products: %w", err)
	}
	for _, product := range products {
		if product.Name == spec.Name {
			return product.ID, nil
		}
	}
	params := url.Values{}
	params.Set("name", spec.Name)
	if spec.Description != "" {
		params.Set("description", spec.Description)
	}
	setMetadata(params, spec.Metadata)
	var out stripeProduct
	if err := c.doStripe(ctx, http.MethodPost, "/v1/products", params, &out); err != nil {
		return "", fmt.Errorf("stripe seed: create product %q: %w", spec.Key, err)
	}
	return out.ID, nil
}

func (c *stripeConnector) reconcilePrice(ctx context.Context, productID string, spec stripePriceSpec) (string, error) {
	prices, err := c.listPrices(ctx, true, false)
	if err != nil {
		return "", fmt.Errorf("stripe seed: list prices: %w", err)
	}
	for _, price := range prices {
		if !samePriceIdentity(price, spec) {
			continue
		}
		if priceMatchesSpec(price, productID, spec) {
			return price.ID, nil
		}
		if err := c.archivePrice(ctx, price.ID); err != nil {
			return "", fmt.Errorf("stripe seed: archive price %s: %w", price.ID, err)
		}
		break
	}
	params := url.Values{}
	params.Set("product", productID)
	params.Set("currency", strings.ToLower(spec.Currency))
	params.Set("unit_amount", strconv.FormatInt(spec.UnitAmount, 10))
	if spec.LookupKey != "" {
		params.Set("lookup_key", spec.LookupKey)
	}
	if spec.Nickname != "" {
		params.Set("nickname", spec.Nickname)
	}
	if spec.Recurring != nil && spec.Recurring.Interval != "" {
		params.Set("recurring[interval]", spec.Recurring.Interval)
		if spec.Recurring.IntervalCount > 0 {
			params.Set("recurring[interval_count]", strconv.FormatInt(spec.Recurring.IntervalCount, 10))
		}
	}
	var out stripePrice
	if err := c.doStripe(ctx, http.MethodPost, "/v1/prices", params, &out); err != nil {
		return "", fmt.Errorf("stripe seed: create price %q: %w", spec.Key, err)
	}
	return out.ID, nil
}

func samePriceIdentity(price stripePrice, spec stripePriceSpec) bool {
	if spec.LookupKey != "" && price.LookupKey == spec.LookupKey {
		return true
	}
	return spec.StripeID != "" && price.ID == spec.StripeID
}

func priceMatchesSpec(price stripePrice, productID string, spec stripePriceSpec) bool {
	if price.Product != productID {
		return false
	}
	if !strings.EqualFold(price.Currency, spec.Currency) || price.UnitAmount != spec.UnitAmount {
		return false
	}
	if spec.Recurring == nil {
		return price.Recurring == nil
	}
	if price.Recurring == nil || price.Recurring.Interval != spec.Recurring.Interval {
		return false
	}
	if spec.Recurring.IntervalCount > 0 && price.Recurring.IntervalCount != spec.Recurring.IntervalCount {
		return false
	}
	return true
}

func (c *stripeConnector) archivePrice(ctx context.Context, id string) error {
	params := url.Values{}
	params.Set("active", "false")
	return c.doStripe(ctx, http.MethodPost, "/v1/prices/"+url.PathEscape(id), params, nil)
}

func (c *stripeConnector) resetManagedObjects(ctx context.Context, reset stripeResetSpec) error {
	if reset.Customers.Mode != "delete_recreate" {
		return fmt.Errorf("stripe seed: unsupported customer reset mode %q", reset.Customers.Mode)
	}
	customers, err := c.listManagedCustomers(ctx, reset.Customers.ManagedMetadataKey, reset.Customers.ManagedMetadataValue)
	if err != nil {
		return fmt.Errorf("stripe seed: list managed customers: %w", err)
	}
	for _, customer := range customers {
		if err := c.doStripe(ctx, http.MethodDelete, "/v1/customers/"+url.PathEscape(customer.ID), nil, nil); err != nil {
			return fmt.Errorf("stripe seed: delete managed customer %s: %w", customer.ID, err)
		}
	}
	return nil
}

type stripeRow struct {
	Table  string
	Header []string
	Values map[string]string
}

func (c *stripeConnector) recreateCustomers(ctx context.Context, dataDir string, snap stripeSnapshot) (map[string]string, map[string]stripeRow, error) {
	objectIDs := map[string]string{}
	rowsByAlias := map[string]stripeRow{}
	for _, spec := range snap.Objects.Customers {
		rows, err := readCSVRows(filepath.Join(dataDir, spec.Source.Table+".csv"))
		if err != nil {
			return nil, nil, fmt.Errorf("stripe seed: read %s.csv: %w", spec.Source.Table, err)
		}
		for _, row := range rows {
			match := strings.TrimSpace(row.Values[spec.Source.MatchColumn])
			if match == "" || strings.EqualFold(match, "NULL") {
				continue
			}
			alias := renderAlias(spec.aliasTemplate(spec.Source.MatchColumn), row.Values)
			if alias == "" {
				return nil, nil, fmt.Errorf("stripe seed: could not render customer alias for table %s", spec.Source.Table)
			}
			params := url.Values{}
			params.Set("email", match)
			metadata := copyStringMap(spec.Metadata)
			if _, ok := metadata[snap.Reset.Customers.ManagedMetadataKey]; !ok {
				metadata[snap.Reset.Customers.ManagedMetadataKey] = snap.Reset.Customers.ManagedMetadataValue
			}
			setMetadata(params, metadata)
		var out stripeCustomer
		if err := c.doStripe(ctx, http.MethodPost, "/v1/customers", params, &out); err != nil {
			return nil, nil, fmt.Errorf("stripe seed: create customer %q: %w", match, err)
		}
		if err := c.attachTestPaymentMethod(ctx, out.ID); err != nil {
			return nil, nil, fmt.Errorf("stripe seed: attach payment method for customer %q: %w", match, err)
		}
		objectIDs[alias] = out.ID
		rowsByAlias[alias] = row
		}
	}
	return objectIDs, rowsByAlias, nil
}

// attachTestPaymentMethod creates a Visa test card, attaches it to the
// customer, and sets it as the invoice default so that new subscriptions
// activate immediately without requiring payment_behavior=default_incomplete.
func (c *stripeConnector) attachTestPaymentMethod(ctx context.Context, customerID string) error {
	pmParams := url.Values{}
	pmParams.Set("type", "card")
	pmParams.Set("card[token]", "tok_visa")
	var pm struct {
		ID string `json:"id"`
	}
	if err := c.doStripe(ctx, http.MethodPost, "/v1/payment_methods", pmParams, &pm); err != nil {
		return fmt.Errorf("create test payment method: %w", err)
	}
	attachParams := url.Values{}
	attachParams.Set("customer", customerID)
	if err := c.doStripe(ctx, http.MethodPost, "/v1/payment_methods/"+url.PathEscape(pm.ID)+"/attach", attachParams, nil); err != nil {
		return fmt.Errorf("attach payment method %s: %w", pm.ID, err)
	}
	updateParams := url.Values{}
	updateParams.Set("invoice_settings[default_payment_method]", pm.ID)
	if err := c.doStripe(ctx, http.MethodPost, "/v1/customers/"+url.PathEscape(customerID), updateParams, nil); err != nil {
		return fmt.Errorf("set default payment method on customer %s: %w", customerID, err)
	}
	return nil
}

func (s stripeCustomerObjectSpec) aliasTemplate(matchColumn string) string {
	if strings.TrimSpace(s.Alias) != "" {
		return s.Alias
	}
	return fmt.Sprintf(stripeDefaultCustomerAlias, matchColumn)
}

func (c *stripeConnector) recreateSubscriptions(ctx context.Context, snap stripeSnapshot, priceIDs map[string]string, objectIDs map[string]string, rowsByAlias map[string]stripeRow) error {
	// Build a reverse index: catalog stripeId → resolved price ID so that
	// subscription specs written with raw Stripe IDs (instead of logical
	// catalog keys) still resolve correctly.
	for _, prod := range snap.Catalog.Products {
		for _, p := range prod.Prices {
			if p.StripeID != "" && priceIDs[p.StripeID] == "" {
				if resolved := priceIDs[p.Key]; resolved != "" {
					priceIDs[p.StripeID] = resolved
				}
			}
		}
	}

	for _, spec := range snap.Objects.Subscriptions {
		priceID := priceIDs[spec.PriceKey]
		if priceID == "" {
			return fmt.Errorf("stripe seed: no resolved Stripe price for priceKey %q", spec.PriceKey)
		}
		for _, row := range rowsByAlias {
			customerAlias := renderAlias(spec.CustomerAlias, row.Values)
			customerID := objectIDs[customerAlias]
			if customerID == "" {
				continue
			}
			alias := renderAlias(spec.Alias, row.Values)
			if alias == "" {
				return fmt.Errorf("stripe seed: could not render subscription alias for customer alias %q", customerAlias)
			}
			params := url.Values{}
			params.Set("customer", customerID)
			params.Set("items[0][price]", priceID)
			params.Set("metadata["+stripeDefaultManagedKey+"]", stripeDefaultManagedValue)
			if spec.TrialPeriodDays > 0 {
				params.Set("trial_period_days", strconv.Itoa(spec.TrialPeriodDays))
			} else if spec.PaymentBehavior != "" {
				params.Set("payment_behavior", spec.PaymentBehavior)
			}
			var out stripeSubscription
			if err := c.doStripe(ctx, http.MethodPost, "/v1/subscriptions", params, &out); err != nil {
				return fmt.Errorf("stripe seed: create subscription for %s: %w", customerAlias, err)
			}
			objectIDs[alias] = out.ID
		}
	}
	return nil
}

func resolveExternalIDs(dataDir string, rules []externalIDResolution, objectIDs map[string]string) error {
	grouped := map[string][]externalIDResolution{}
	for _, rule := range rules {
		if rule.Table == "" || rule.MatchColumn == "" || rule.OutputColumn == "" || rule.ObjectAlias == "" {
			return fmt.Errorf("stripe seed: invalid externalIdResolution rule for table %q", rule.Table)
		}
		grouped[rule.Table] = append(grouped[rule.Table], rule)
	}
	for table, tableRules := range grouped {
		path := filepath.Join(dataDir, table+".csv")
		header, records, err := readCSVFile(path)
		if err != nil {
			return fmt.Errorf("stripe seed: read %s.csv: %w", table, err)
		}
		index := headerIndex(header)
		for i := range records {
			row := recordMap(header, records[i])
			for _, rule := range tableRules {
				if strings.TrimSpace(row[rule.MatchColumn]) == "" {
					continue
				}
				alias := renderAlias(rule.ObjectAlias, row)
				id := objectIDs[alias]
				if id == "" {
					return fmt.Errorf("stripe seed: no resolved external ID for alias %q (table %s)", alias, table)
				}
				col, ok := index[rule.OutputColumn]
				if !ok {
					return fmt.Errorf("stripe seed: table %s has no output column %q", table, rule.OutputColumn)
				}
				records[i][col] = id
			}
		}
		if err := writeCSVFile(path, header, records); err != nil {
			return fmt.Errorf("stripe seed: write %s.csv: %w", table, err)
		}
	}
	return nil
}

func renderAlias(template string, row map[string]string) string {
	out := template
	for key, value := range row {
		out = strings.ReplaceAll(out, "{"+key+"}", value)
	}
	if strings.Contains(out, "{") || strings.Contains(out, "}") {
		return ""
	}
	return out
}

func readCSVRows(path string) ([]stripeRow, error) {
	header, records, err := readCSVFile(path)
	if err != nil {
		return nil, err
	}
	rows := make([]stripeRow, 0, len(records))
	table := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	for _, record := range records {
		rows = append(rows, stripeRow{Table: table, Header: header, Values: recordMap(header, record)})
	}
	return rows, nil
}

func readCSVFile(path string) ([]string, [][]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	reader := csv.NewReader(bytes.NewReader(data))
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("empty CSV")
	}
	return rows[0], rows[1:], nil
}

func writeCSVFile(path string, header []string, records [][]string) error {
	tmp := path + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := writer.WriteAll(records); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func headerIndex(header []string) map[string]int {
	out := make(map[string]int, len(header))
	for i, column := range header {
		out[column] = i
	}
	return out
}

func recordMap(header, record []string) map[string]string {
	out := make(map[string]string, len(header))
	for i, column := range header {
		if i < len(record) {
			out[column] = record[i]
		}
	}
	return out
}

func (c *stripeConnector) listProducts(ctx context.Context, activeOnly, includeArchived bool) ([]stripeProduct, error) {
	params := url.Values{}
	params.Set("limit", "100")
	if activeOnly && !includeArchived {
		params.Set("active", "true")
	}
	var out struct {
		Data []stripeProduct `json:"data"`
	}
	if err := c.doStripe(ctx, http.MethodGet, "/v1/products", params, &out); err != nil {
		return nil, err
	}
	if includeArchived {
		return out.Data, nil
	}
	filtered := make([]stripeProduct, 0, len(out.Data))
	for _, product := range out.Data {
		if product.Active {
			filtered = append(filtered, product)
		}
	}
	return filtered, nil
}

func (c *stripeConnector) retrieveProduct(ctx context.Context, id string) (stripeProduct, error) {
	var out stripeProduct
	err := c.doStripe(ctx, http.MethodGet, "/v1/products/"+url.PathEscape(id), nil, &out)
	return out, err
}

func (c *stripeConnector) listPrices(ctx context.Context, activeOnly, includeArchived bool) ([]stripePrice, error) {
	params := url.Values{}
	params.Set("limit", "100")
	if activeOnly && !includeArchived {
		params.Set("active", "true")
	}
	var out struct {
		Data []stripePrice `json:"data"`
	}
	if err := c.doStripe(ctx, http.MethodGet, "/v1/prices", params, &out); err != nil {
		return nil, err
	}
	if includeArchived {
		return out.Data, nil
	}
	filtered := make([]stripePrice, 0, len(out.Data))
	for _, price := range out.Data {
		if price.Active {
			filtered = append(filtered, price)
		}
	}
	return filtered, nil
}

func (c *stripeConnector) listManagedCustomers(ctx context.Context, key, value string) ([]stripeCustomer, error) {
	all, err := c.listAllCustomers(ctx)
	if err != nil {
		return nil, err
	}
	return filterManaged(all, key, value), nil
}

func (c *stripeConnector) listAllCustomers(ctx context.Context) ([]stripeCustomer, error) {
	params := url.Values{}
	params.Set("limit", "100")
	var out struct {
		Data []stripeCustomer `json:"data"`
	}
	if err := c.doStripe(ctx, http.MethodGet, "/v1/customers", params, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func filterManaged(customers []stripeCustomer, key, value string) []stripeCustomer {
	out := make([]stripeCustomer, 0, len(customers))
	for _, c := range customers {
		if c.Metadata[key] == value {
			out = append(out, c)
		}
	}
	return out
}

func (c *stripeConnector) listSubscriptions(ctx context.Context) ([]stripeSubscription, error) {
	params := url.Values{}
	params.Set("limit", "100")
	var out struct {
		Data []stripeSubscription `json:"data"`
	}
	if err := c.doStripe(ctx, http.MethodGet, "/v1/subscriptions", params, &out); err != nil {
		return nil, err
	}
	filtered := make([]stripeSubscription, 0, len(out.Data))
	for _, subscription := range out.Data {
		if activeLikeSubscription(subscription.Status) {
			filtered = append(filtered, subscription)
		}
	}
	return filtered, nil
}

func activeLikeSubscription(status string) bool {
	switch status {
	case "active", "trialing", "past_due", "incomplete":
		return true
	default:
		return false
	}
}

func (c *stripeConnector) doStripe(ctx context.Context, method, path string, params url.Values, out interface{}) error {
	target := c.apiBase + path
	var body io.Reader
	if method == http.MethodGet && len(params) > 0 {
		target += "?" + params.Encode()
	} else if len(params) > 0 {
		body = strings.NewReader(params.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if method != http.MethodGet && len(params) > 0 {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func buildStripeCatalog(existing stripeCatalogSpec, products []stripeProduct, prices []stripePrice) stripeCatalogSpec {
	existingByProduct := map[string]stripeProductSpec{}
	for _, product := range existing.Products {
		existingByProduct[product.StripeID] = product
	}
	pricesByProduct := map[string][]stripePrice{}
	for _, price := range prices {
		pricesByProduct[price.Product] = append(pricesByProduct[price.Product], price)
	}
	out := stripeCatalogSpec{
		ActiveOnly:      true,
		IncludeArchived: existing.IncludeArchived,
	}
	for _, product := range products {
		key := stableKey(product.Name)
		if existing, ok := existingByProduct[product.ID]; ok && existing.Key != "" {
			key = existing.Key
		}
		spec := stripeProductSpec{
			Key:         key,
			StripeID:    product.ID,
			Name:        product.Name,
			Description: product.Description,
			Active:      product.Active,
			Metadata:    product.Metadata,
		}
		// Deduplicate by key: if two prices hash to the same key (e.g. the
		// original price and a replacement created by a previous reconcile),
		// keep the one with the larger Stripe ID (most recently created).
		seenPriceKey := map[string]int{} // key -> index in spec.Prices
		for _, price := range pricesByProduct[product.ID] {
			priceKey := price.LookupKey
			if priceKey == "" {
				priceKey = stableKey(strings.Join([]string{product.Name, price.Nickname, price.Currency, strconv.FormatInt(price.UnitAmount, 10)}, "-"))
			}
			entry := stripePriceSpec{
				Key:        priceKey,
				StripeID:   price.ID,
				LookupKey:  price.LookupKey,
				Currency:   price.Currency,
				UnitAmount: price.UnitAmount,
				Nickname:   price.Nickname,
				Active:     price.Active,
				Recurring:  price.Recurring,
			}
			if idx, exists := seenPriceKey[priceKey]; exists {
				if price.ID > spec.Prices[idx].StripeID {
					spec.Prices[idx] = entry
				}
			} else {
				seenPriceKey[priceKey] = len(spec.Prices)
				spec.Prices = append(spec.Prices, entry)
			}
		}
		out.Products = append(out.Products, spec)
	}
	return out
}

func stableKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func customerSnapshots(customers []stripeCustomer) []stripeCustomerSnap {
	out := make([]stripeCustomerSnap, 0, len(customers))
	for _, customer := range customers {
		out = append(out, stripeCustomerSnap{
			Email:    customer.Email,
			StripeID: customer.ID,
			Metadata: customer.Metadata,
		})
	}
	return out
}

func subscriptionSnapshots(subscriptions []stripeSubscription, customers []stripeCustomer, priceIDToKey map[string]string) []stripeSubscriptionSnap {
	customersByID := map[string]stripeCustomer{}
	for _, customer := range customers {
		customersByID[customer.ID] = customer
	}
	out := make([]stripeSubscriptionSnap, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		priceKey := ""
		if len(subscription.Items.Data) == 1 {
			priceID := subscription.Items.Data[0].Price.ID
			if k, ok := priceIDToKey[priceID]; ok {
				priceKey = k
			} else {
				priceKey = priceID // fallback: raw ID
			}
		}
		out = append(out, stripeSubscriptionSnap{
			StripeID:      subscription.ID,
			Customer:      subscription.Customer,
			CustomerEmail: customersByID[subscription.Customer].Email,
			PriceKey:      priceKey,
			Status:        subscription.Status,
		})
	}
	return out
}

type inferredSpecs struct {
	Customers     []stripeCustomerObjectSpec
	Subscriptions []stripeSubscriptionObjectSpec
}

func inferStripeResolution(dataDir string, snap stripeSnapshot) ([]externalIDResolution, inferredSpecs) {
	customerByID := map[string]stripeCustomerSnap{}
	for _, customer := range snap.Customers {
		if customer.StripeID != "" {
			customerByID[customer.StripeID] = customer
		}
	}
	subByID := map[string]stripeSubscriptionSnap{}
	for _, subscription := range snap.Subscriptions {
		if subscription.StripeID != "" {
			subByID[subscription.StripeID] = subscription
		}
	}
	var rules []externalIDResolution
	specs := inferredSpecs{}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return rules, specs
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}
		table := strings.TrimSuffix(entry.Name(), ".csv")
		header, records, err := readCSVFile(filepath.Join(dataDir, entry.Name()))
		if err != nil {
			continue
		}
		for _, record := range records {
			row := recordMap(header, record)
			for column, value := range row {
				if customer, ok := customerByID[value]; ok && customer.Email != "" {
					matchColumn := findColumnWithValue(row, customer.Email)
					if matchColumn == "" {
						continue
					}
					alias := "customer:{" + matchColumn + "}"
					rules = append(rules, externalIDResolution{Table: table, MatchColumn: matchColumn, OutputColumn: column, ObjectAlias: alias})
					specs.Customers = append(specs.Customers, stripeCustomerObjectSpec{
						Alias:  alias,
						Source: stripeSourceSpec{Table: table, MatchColumn: matchColumn},
						Metadata: map[string]string{
							stripeDefaultManagedKey: stripeDefaultManagedValue,
						},
					})
				}
				if subscription, ok := subByID[value]; ok && subscription.CustomerEmail != "" && subscription.PriceKey != "" {
					matchColumn := findColumnWithValue(row, subscription.CustomerEmail)
					if matchColumn == "" {
						continue
					}
					customerAlias := "customer:{" + matchColumn + "}"
					subAlias := "subscription:{" + matchColumn + "}:" + subscription.PriceKey
					rules = append(rules, externalIDResolution{Table: table, MatchColumn: matchColumn, OutputColumn: column, ObjectAlias: subAlias})
					specs.Subscriptions = append(specs.Subscriptions, stripeSubscriptionObjectSpec{
						Alias:         subAlias,
						CustomerAlias: customerAlias,
						PriceKey:      subscription.PriceKey,
						Source:        stripeSourceSpec{Table: table, MatchColumn: matchColumn},
					})
				}
			}
		}
	}
	return dedupeResolutionRules(rules), specs
}

func findColumnWithValue(row map[string]string, value string) string {
	var matches []string
	for column, cell := range row {
		if strings.EqualFold(strings.TrimSpace(cell), strings.TrimSpace(value)) {
			matches = append(matches, column)
		}
	}
	sort.Strings(matches)
	if len(matches) == 1 {
		return matches[0]
	}
	return ""
}

// mergeResolutionRules merges multiple rule sources in priority order:
// the first source wins on `resolutionKey` collisions, so callers should
// pass user-defined rules first, then AI-inferred, then heuristic — that
// way manual overrides are never silently overwritten.
func mergeResolutionRules(sources ...[]externalIDResolution) []externalIDResolution {
	out := []externalIDResolution{}
	seen := map[string]bool{}
	for _, src := range sources {
		for _, rule := range src {
			key := resolutionKey(rule)
			if !seen[key] {
				out = append(out, rule)
				seen[key] = true
			}
		}
	}
	return out
}

// mergeInferred combines multiple inferredSpecs in priority order: the
// first source's customer/subscription specs win on alias collisions.
func mergeInferred(sources ...inferredSpecs) inferredSpecs {
	out := inferredSpecs{}
	customerSeen := map[string]bool{}
	subSeen := map[string]bool{}
	for _, src := range sources {
		for _, spec := range src.Customers {
			if !customerSeen[spec.Alias] {
				out.Customers = append(out.Customers, spec)
				customerSeen[spec.Alias] = true
			}
		}
		for _, spec := range src.Subscriptions {
			if !subSeen[spec.Alias] {
				out.Subscriptions = append(out.Subscriptions, spec)
				subSeen[spec.Alias] = true
			}
		}
	}
	return out
}

func dedupeResolutionRules(rules []externalIDResolution) []externalIDResolution {
	seen := map[string]bool{}
	out := make([]externalIDResolution, 0, len(rules))
	for _, rule := range rules {
		key := resolutionKey(rule)
		if !seen[key] {
			out = append(out, rule)
			seen[key] = true
		}
	}
	return out
}

func resolutionKey(rule externalIDResolution) string {
	return strings.Join([]string{rule.Table, rule.MatchColumn, rule.OutputColumn, rule.ObjectAlias}, "\x00")
}

func mergeObjectSpecs(existing stripeObjectSpecs, inferred inferredSpecs) stripeObjectSpecs {
	out := existing
	customerSeen := map[string]bool{}
	for _, spec := range out.Customers {
		customerSeen[spec.Alias] = true
	}
	for _, spec := range inferred.Customers {
		if !customerSeen[spec.Alias] {
			out.Customers = append(out.Customers, spec)
			customerSeen[spec.Alias] = true
		}
	}
	subSeen := map[string]bool{}
	for _, spec := range out.Subscriptions {
		subSeen[spec.Alias] = true
	}
	for _, spec := range inferred.Subscriptions {
		if !subSeen[spec.Alias] {
			out.Subscriptions = append(out.Subscriptions, spec)
			subSeen[spec.Alias] = true
		}
	}
	return out
}

// logAIInferWarning prints a single-line warning when the AI inference
// call fails. We intentionally write to stderr (not stdout) so the MCP
// stdio transport stays clean. The message is short on purpose — the
// detailed error is in the wrapped err already, and verbose plumbing
// belongs in the CLI layer, not the connector.
func logAIInferWarning(serviceName string, err error) {
	fmt.Fprintf(os.Stderr, "[%s] AI mapping inference skipped: %v\n", serviceName, err)
}

func setMetadata(params url.Values, metadata map[string]string) {
	for key, value := range metadata {
		params.Set("metadata["+key+"]", value)
	}
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
