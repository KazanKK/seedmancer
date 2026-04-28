package services

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/utils"
	_ "github.com/lib/pq" // postgres driver
)

// ─── Snapshot types ───────────────────────────────────────────────────────────

// supabaseAuthSnapshot is the on-disk representation of a Supabase Auth
// service state. It captures the users that should exist after a seed and
// the default password to use when recreating them (real passwords are
// bcrypt-hashed server-side and cannot be exported).
type supabaseAuthSnapshot struct {
	Version         int                  `json:"version"`
	Service         string               `json:"service"`
	CapturedAt      time.Time            `json:"capturedAt"`
	DefaultPassword string               `json:"defaultPassword"`
	Users           []supabaseAuthUser   `json:"users"`
}

type supabaseAuthUser struct {
	// ID is the Supabase auth UUID. Preserved so that on seed we recreate
	// users with the same UUID, keeping any FK references in public.* tables
	// (e.g. a "users" profile table that mirrors auth.users) consistent with
	// the CSV data that will be restored alongside.
	ID              string                 `json:"id,omitempty"`
	Email           string                 `json:"email"`
	Phone           string                 `json:"phone,omitempty"`
	EmailConfirmed  bool                   `json:"emailConfirmed"`
	PhoneConfirmed  bool                   `json:"phoneConfirmed,omitempty"`
	UserMetadata    map[string]interface{} `json:"userMetadata,omitempty"`
	AppMetadata     map[string]interface{} `json:"appMetadata,omitempty"`
}

// ─── connector ───────────────────────────────────────────────────────────────

type supabaseAuthConnector struct {
	name            string
	url             string // e.g. "https://xyzzy.supabase.co"
	serviceRoleKey  string
	httpClient      *http.Client
}

func newSupabaseAuth(name string, cfg utils.ServiceConfig) (*supabaseAuthConnector, error) {
	urlRaw := strings.TrimSpace(cfg.URLEnv)
	if urlRaw == "" {
		urlRaw = "SUPABASE_URL"
	}
	keyRaw := strings.TrimSpace(cfg.ServiceRoleKeyEnv)
	if keyRaw == "" {
		keyRaw = "SUPABASE_SERVICE_ROLE_KEY"
	}

	projectURL, err := resolveValue(name, "url_env", urlRaw)
	if err != nil {
		return nil, err
	}
	serviceRoleKey, err := resolveValue(name, "service_role_key_env", keyRaw)
	if err != nil {
		return nil, err
	}

	return &supabaseAuthConnector{
		name:           name,
		url:            strings.TrimRight(projectURL, "/"),
		serviceRoleKey: serviceRoleKey,
		httpClient:     &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (c *supabaseAuthConnector) ServiceType() string { return "supabase-auth" }

// Export fetches all auth users via the Supabase Admin API and serialises them
// to JSON. Passwords are omitted (bcrypt hashes are not portable), so Seed
// will use the snapshot's DefaultPassword for all recreated users.
func (c *supabaseAuthConnector) Export(ctx context.Context) ([]byte, error) {
	users, err := c.listAllUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("supabase-auth export: list users: %w", err)
	}

	snap := supabaseAuthSnapshot{
		Version:         1,
		Service:         "supabase-auth",
		CapturedAt:      time.Now().UTC(),
		DefaultPassword: "SeedmancerTest1!",
		Users:           users,
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("supabase-auth export: marshal: %w", err)
	}
	return data, nil
}

// Seed syncs auth users to match the snapshot using an incremental approach:
//   - Users in snapshot that already exist in auth (matched by UUID): PATCH (update)
//   - Users in snapshot that don't exist in auth: POST (create)
//   - Users in auth that are not in snapshot: DELETE
//
// PATCH never fires INSERT triggers (important: many Supabase projects have a
// trigger on auth.users INSERT that writes to a public.users mirror table).
// If we used wipe+recreate, the trigger would fire on an already-populated
// mirror table and hit a duplicate-key constraint. The incremental approach
// avoids this entirely.
func (c *supabaseAuthConnector) Seed(ctx context.Context, snapshot []byte) error {
	var snap supabaseAuthSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return fmt.Errorf("supabase-auth seed: unmarshal snapshot: %w", err)
	}
	if snap.Version != 1 {
		return fmt.Errorf("supabase-auth seed: unsupported snapshot version %d", snap.Version)
	}

	// Fetch the current auth state (id + email).
	existing, err := c.listAllUsersWithID(ctx)
	if err != nil {
		return fmt.Errorf("supabase-auth seed: list existing users: %w", err)
	}
	existingByID := make(map[string]userRecord, len(existing))
	existingByEmail := make(map[string]userRecord, len(existing))
	for _, u := range existing {
		existingByID[u.id] = u
		if u.email != "" {
			existingByEmail[strings.ToLower(u.email)] = u
		}
	}

	password := strings.TrimSpace(snap.DefaultPassword)
	if password == "" {
		password = "SeedmancerTest1!"
	}

	// Open an optional direct DB connection for pre-creation cleanup.
	// Many Supabase projects have a trigger on auth.users INSERT that mirrors
	// new users into a public.users table. If that mirror table already holds
	// a row for the email we're about to insert (e.g. restored from a CSV
	// snapshot), the trigger will fire and hit a duplicate-key error, rolling
	// back the entire auth.users INSERT. We side-step this by deleting any
	// matching mirror rows just before each new auth user creation; the DB
	// seed that runs immediately after will restore those rows from the CSV
	// anyway, landing in the exact same final state.
	var directDB *sql.DB
	if dbURL, ok := DBURLFromContext(ctx); ok {
		// Local Postgres may not have SSL; append sslmode=disable if no ssl
		// config is already present in the URL.
		connStr := dbURL
		if !strings.Contains(connStr, "sslmode") {
			if strings.Contains(connStr, "?") {
				connStr += "&sslmode=disable"
			} else {
				connStr += "?sslmode=disable"
			}
		}
		db, err := sql.Open("postgres", connStr)
		if err == nil {
			directDB = db
			defer directDB.Close()
		}
	}

	// Build the target set (IDs we want after seed).
	wantIDs := make(map[string]bool, len(snap.Users))
	for _, u := range snap.Users {
		wantIDs[u.ID] = true
	}

	// 1. PATCH users that already exist; POST users that are new.
	for _, u := range snap.Users {
		// Match by UUID first; fall back to email for snapshots captured
		// before UUID preservation was added.
		existingRecord, alreadyExists := existingByID[u.ID]
		if !alreadyExists && u.Email != "" {
			existingRecord, alreadyExists = existingByEmail[strings.ToLower(u.Email)]
			if alreadyExists {
				// Backfill the UUID so wantIDs tracking stays consistent.
				u.ID = existingRecord.id
			}
		}

		if alreadyExists {
			// User with this UUID already exists → update without triggering INSERT.
			if u.ID == "" {
				u.ID = existingRecord.id
			}
			if err := c.updateUser(ctx, u, password); err != nil {
				return fmt.Errorf("supabase-auth seed: update user %s: %w", u.Email, err)
			}
		} else {
			// Genuinely new user. Pre-clean any mirror rows that would cause a
			// trigger-level duplicate-key error on creation.
			if directDB != nil && u.Email != "" {
				_ = cleanMirrorRows(directDB, u.Email) // best-effort
			}
			if err := c.createUser(ctx, u, password); err != nil {
				return fmt.Errorf("supabase-auth seed: create user %s: %w", u.Email, err)
			}
		}
	}

	// 2. DELETE users not in the snapshot.
	for _, u := range existing {
		if !wantIDs[u.id] {
			if err := c.deleteUser(ctx, u.id); err != nil {
				return fmt.Errorf("supabase-auth seed: delete removed user %s: %w", u.email, err)
			}
		}
	}
	return nil
}

// ─── Supabase Admin REST helpers ─────────────────────────────────────────────

// userRecord holds the id and email for a single admin-list entry.
type userRecord struct {
	id    string
	email string
}

// listAllUsersWithID pages through GET /auth/v1/admin/users and returns id+email
// pairs. A single list call now replaces the two separate passes (listAllUsers
// and listAllUserIDs) that existed before.
func (c *supabaseAuthConnector) listAllUsersWithID(ctx context.Context) ([]userRecord, error) {
	type adminUser struct {
		ID               string                 `json:"id"`
		Email            string                 `json:"email"`
		Phone            string                 `json:"phone"`
		EmailConfirmedAt *string                `json:"email_confirmed_at"`
		PhoneConfirmedAt *string                `json:"phone_confirmed_at"`
		UserMetadata     map[string]interface{} `json:"user_metadata"`
		AppMetadata      map[string]interface{} `json:"app_metadata"`
	}
	type listResponse struct {
		Users []adminUser `json:"users"`
		Total int         `json:"total"`
	}

	var all []userRecord
	page := 1
	perPage := 1000
	for {
		url := fmt.Sprintf("%s/auth/v1/admin/users?page=%d&per_page=%d", c.url, page, perPage)
		var resp listResponse
		if err := c.doJSON(ctx, http.MethodGet, url, nil, &resp); err != nil {
			return nil, err
		}
		for _, u := range resp.Users {
			all = append(all, userRecord{id: u.ID, email: u.Email})
		}
		if len(resp.Users) < perPage {
			break
		}
		page++
	}
	return all, nil
}

// listAllUsers is kept for Export — it returns the user shape without the raw password.
func (c *supabaseAuthConnector) listAllUsers(ctx context.Context) ([]supabaseAuthUser, error) {
	type adminUser struct {
		ID               string                 `json:"id"`
		Email            string                 `json:"email"`
		Phone            string                 `json:"phone"`
		EmailConfirmedAt *string                `json:"email_confirmed_at"`
		PhoneConfirmedAt *string                `json:"phone_confirmed_at"`
		UserMetadata     map[string]interface{} `json:"user_metadata"`
		AppMetadata      map[string]interface{} `json:"app_metadata"`
	}
	type listResponse struct {
		Users []adminUser `json:"users"`
		Total int         `json:"total"`
	}

	var all []supabaseAuthUser
	page := 1
	perPage := 1000
	for {
		url := fmt.Sprintf("%s/auth/v1/admin/users?page=%d&per_page=%d", c.url, page, perPage)
		var resp listResponse
		if err := c.doJSON(ctx, http.MethodGet, url, nil, &resp); err != nil {
			return nil, err
		}
		for _, u := range resp.Users {
			mapped := supabaseAuthUser{
				ID:             u.ID,
				Email:          u.Email,
				Phone:          u.Phone,
				EmailConfirmed: u.EmailConfirmedAt != nil,
				PhoneConfirmed: u.PhoneConfirmedAt != nil,
				UserMetadata:   u.UserMetadata,
				AppMetadata:    u.AppMetadata,
			}
			all = append(all, mapped)
		}
		if len(resp.Users) < perPage {
			break
		}
		page++
	}
	return all, nil
}

// findUserByEmail scans the admin user list for a user with the given email
// and returns their UUID. Returns ("", nil) if not found.
func (c *supabaseAuthConnector) findUserByEmail(ctx context.Context, email string) (string, error) {
	records, err := c.listAllUsersWithID(ctx)
	if err != nil {
		return "", err
	}
	for _, r := range records {
		if strings.EqualFold(r.email, email) {
			return r.id, nil
		}
	}
	return "", nil
}

// updateUser PATCHes an existing auth user's metadata and password without
// triggering any INSERT triggers on the database.
func (c *supabaseAuthConnector) updateUser(ctx context.Context, u supabaseAuthUser, password string) error {
	type updateReq struct {
		Email        string                 `json:"email,omitempty"`
		Phone        string                 `json:"phone,omitempty"`
		Password     string                 `json:"password,omitempty"`
		EmailConfirm bool                   `json:"email_confirm"`
		UserMetadata map[string]interface{} `json:"user_metadata,omitempty"`
		AppMetadata  map[string]interface{} `json:"app_metadata,omitempty"`
	}
	body := updateReq{
		Email:        u.Email,
		Phone:        u.Phone,
		EmailConfirm: u.EmailConfirmed,
		UserMetadata: u.UserMetadata,
		AppMetadata:  u.AppMetadata,
	}
	if u.Email != "" {
		body.Password = password
	}
	url := fmt.Sprintf("%s/auth/v1/admin/users/%s", c.url, u.ID)
	return c.doJSON(ctx, http.MethodPut, url, body, nil)
}

// cleanMirrorRows deletes rows from ALL public tables that have an `email`
// column and hold the given email address. This is a best-effort helper: if
// a project's auth.users INSERT trigger mirrors users into a public.* profile
// table, the mirror row must be absent when the trigger fires, otherwise the
// INSERT into auth.users rolls back with a duplicate-key error.
//
// After auth seed completes, the DB seed restores all public.* tables from
// their CSV snapshots, so any rows removed here are put back with the
// correct data.
func cleanMirrorRows(db *sql.DB, email string) error {
	rows, err := db.Query(`
		SELECT table_name FROM information_schema.columns
		WHERE table_schema = 'public' AND column_name = 'email'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err == nil {
			tables = append(tables, t)
		}
	}
	for _, t := range tables {
		_, _ = db.Exec(fmt.Sprintf(`DELETE FROM public.%q WHERE email = $1`, t), email)
	}
	return nil
}

func (c *supabaseAuthConnector) deleteUser(ctx context.Context, id string) error {
	url := fmt.Sprintf("%s/auth/v1/admin/users/%s", c.url, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s → %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *supabaseAuthConnector) createUser(ctx context.Context, u supabaseAuthUser, password string) error {
	type createReq struct {
		ID              string                 `json:"id,omitempty"`
		Email           string                 `json:"email,omitempty"`
		Phone           string                 `json:"phone,omitempty"`
		Password        string                 `json:"password,omitempty"`
		EmailConfirm    bool                   `json:"email_confirm"`
		PhoneConfirm    bool                   `json:"phone_confirm,omitempty"`
		UserMetadata    map[string]interface{} `json:"user_metadata,omitempty"`
		AppMetadata     map[string]interface{} `json:"app_metadata,omitempty"`
	}

	body := createReq{
		ID:           u.ID, // preserve original UUID so public.* FK refs stay valid
		Email:        u.Email,
		Phone:        u.Phone,
		EmailConfirm: u.EmailConfirmed,
		PhoneConfirm: u.PhoneConfirmed,
		UserMetadata: u.UserMetadata,
		AppMetadata:  u.AppMetadata,
	}
	if u.Email != "" {
		body.Password = password
	}

	url := fmt.Sprintf("%s/auth/v1/admin/users", c.url)
	return c.doJSON(ctx, http.MethodPost, url, body, nil)
}

// doJSON is a thin wrapper around http.Client that sends JSON and optionally
// decodes the response JSON into out (pass nil to discard the body).
func (c *supabaseAuthConnector) doJSON(ctx context.Context, method, url string, body interface{}, out interface{}) error {
	var reqBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return err
	}
	c.setHeaders(req)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s → %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func (c *supabaseAuthConnector) setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.serviceRoleKey)
	req.Header.Set("apikey", c.serviceRoleKey)
}
