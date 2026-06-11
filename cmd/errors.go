package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
)

// limitErrorResponse matches the JSON body returned by the Seedmancer API
// when a request is rejected due to a plan limit (HTTP 402).
type limitErrorResponse struct {
	Errors []struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"errors"`
	Limit *struct {
		Kind     string `json:"kind"`
		Used     int    `json:"used"`
		Limit    int    `json:"limit"`
		PlanType string `json:"planType"`
		Message  string `json:"message"`
		CTA      string `json:"cta"`
	} `json:"limit"`
}

// formatLimitError renders a friendly, multi-line error message from a 402
// JSON body. The raw body is used as a fallback when parsing fails so the
// CLI never swallows useful diagnostic info.
func formatLimitError(body []byte) error {
	var parsed limitErrorResponse
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Limit == nil {
		return fmt.Errorf("plan limit reached: %s", strings.TrimSpace(string(body)))
	}
	lim := parsed.Limit
	lines := []string{lim.Message}
	if lim.CTA != "" {
		lines = append(lines, lim.CTA)
	}
	if strings.EqualFold(lim.PlanType, "free") {
		lines = append(lines, "Upgrade at https://seedmancer.dev/dashboard/billing")
	}
	return fmt.Errorf("%s", strings.Join(lines, "\n  "))
}
