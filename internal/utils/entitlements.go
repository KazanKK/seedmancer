package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ErrServiceConnectorsPro is returned when the user's plan does not include
// service connectors. The error message is human-readable and links to the
// upgrade page so the CLI can surface it directly.
var ErrServiceConnectorsPro = fmt.Errorf(
	"service connectors (Supabase Auth) require a Pro plan\n" +
		"  Upgrade at https://seedmancer.dev/dashboard/billing",
)

// planResponse mirrors the JSON from GET /v1.0/plan.
type planResponse struct {
	Plan   string `json:"plan"`
	Limits struct {
		ServiceConnectors bool `json:"serviceConnectors"`
	} `json:"limits"`
}

// CheckServiceConnectorEntitlement contacts the Seedmancer API and returns:
//   - nil              → Pro user, service connectors enabled
//   - ErrMissingAPIToken / ErrInvalidAPIToken → not logged in
//   - ErrServiceConnectorsPro → logged in but on Free plan
//   - any other error  → API unreachable / unexpected response
//
// Commands use this before running export/seed service steps. When the user
// is not logged in the CLI can choose to warn (export) or silently skip
// (seed), depending on context.
func CheckServiceConnectorEntitlement(baseURL, token string) error {
	if token == "" {
		return ErrMissingAPIToken
	}

	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1.0/plan", nil)
	if err != nil {
		return fmt.Errorf("plan check: %w", err)
	}
	req.Header.Set("Authorization", BearerAPIToken(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Network error — treat as unknown, don't block local ops.
		return fmt.Errorf("plan check: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusUnauthorized {
		return ErrInvalidAPIToken
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("plan check: unexpected status %s: %s", resp.Status, string(body))
	}

	var plan planResponse
	if err := json.Unmarshal(body, &plan); err != nil {
		return fmt.Errorf("plan check: malformed response: %w", err)
	}

	if !plan.Limits.ServiceConnectors {
		return ErrServiceConnectorsPro
	}
	return nil
}
