package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/KazanKK/seedmancer/internal/ui"
	utils "github.com/KazanKK/seedmancer/internal/utils"
	"github.com/urfave/cli/v2"
)

// LoginCommand authenticates the CLI via a browser-based flow and stores the
// resulting API token in ~/.seedmancer/credentials (a secret-only file
// separate from config.yaml so project config can be committed safely).
//
// Flow:
//  1. CLI starts a one-shot HTTP server on 127.0.0.1:<random port>.
//  2. CLI opens the dashboard's /auth/cli page with a unique code and the
//     localhost callback URL.
//  3. The dashboard, after the user confirms, calls the callback URL with
//     ?code=<code>&token=<token>.
//  4. CLI validates the code, saves the token, and exits.
//
// Tokens never leave the user's machine: the callback listens on the
// loopback interface only and the page running the exchange is served from
// the Seedmancer dashboard the user is already signed in to.
func LoginCommand() *cli.Command {
	return &cli.Command{
		Name:  "login",
		Usage: "Sign in with your browser and save an API token locally",
		Description: "Opens https://seedmancer.dev/auth/cli in your browser, waits for the\n" +
			"dashboard to hand back a scoped API token, and writes it to\n" +
			"~/.seedmancer/credentials (0600) so subsequent commands just work.\n" +
			"Tokens are kept out of seedmancer.yaml / config.yaml by design.\n\n" +
			"Use --token if you already have a token and want to skip the browser.",
		ArgsUsage: " ",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "token",
				Usage: "Existing API token to save (skips the browser flow)",
			},
			&cli.StringFlag{
				Name:    "dashboard-url",
				Usage:   "Dashboard origin serving /auth/cli (overrides SEEDMANCER_DASHBOARD_URL)",
				EnvVars: []string{"SEEDMANCER_DASHBOARD_URL"},
			},
			&cli.BoolFlag{
				Name:  "no-browser",
				Usage: "Print the URL instead of opening a browser (useful on remote shells)",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Value: 5 * time.Minute,
				Usage: "How long to wait for the browser to complete the flow",
			},
		},
		Action: runLogin,
	}
}

func runLogin(c *cli.Context) error {
	if manual := strings.TrimSpace(c.String("token")); manual != "" {
		if err := utils.SaveAPICredentials(manual); err != nil {
			return fmt.Errorf("saving token: %w", err)
		}
		ui.Success("Saved API token to ~/.seedmancer/credentials")
		warnIfEnvTokenShadows(manual)
		return nil
	}

	dashboard := resolveDashboardURL(c.String("dashboard-url"))

	code, err := randomCode()
	if err != nil {
		return fmt.Errorf("generating code: %w", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("opening loopback listener: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	callbackURL := fmt.Sprintf("http://127.0.0.1:%d/callback", port)

	type result struct {
		token string
		err   error
	}
	done := make(chan result, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		gotCode := q.Get("code")
		token := q.Get("token")

		if gotCode == "" || token == "" {
			renderCallback(w, false, "Missing code or token in callback.")
			done <- result{err: errors.New("callback missing code or token")}
			return
		}
		if gotCode != code {
			renderCallback(w, false, "Authorization code did not match. Please re-run seedmancer login.")
			done <- result{err: errors.New("code mismatch; refusing to save token")}
			return
		}
		renderCallback(w, true, "You can close this tab and return to your terminal.")
		done <- result{token: token}
	})

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- result{err: fmt.Errorf("callback server failed: %w", err)}
		}
	}()

	authURL := buildAuthURL(dashboard, code, callbackURL)
	if c.Bool("no-browser") {
		ui.Info("Open this URL in a browser to continue:\n  %s", authURL)
	} else {
		ui.Info("Opening your browser to sign in...")
		if err := openBrowser(authURL); err != nil {
			ui.Warn("Could not open browser automatically: %v", err)
			ui.Info("Open this URL in a browser to continue:\n  %s", authURL)
		}
	}

	spinner := ui.StartSpinner("Waiting for browser authorization...")

	timeout := c.Duration("timeout")
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}

	select {
	case res := <-done:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = server.Shutdown(shutdownCtx)
		cancel()
		if res.err != nil {
			spinner.Stop(false, "Sign-in failed")
			return res.err
		}
		if err := utils.SaveAPICredentials(res.token); err != nil {
			spinner.Stop(false, "Could not save token")
			return fmt.Errorf("saving token: %w", err)
		}
		spinner.Stop(true, "Signed in")
		ui.Success("Saved API token to ~/.seedmancer/credentials")
		warnIfEnvTokenShadows(res.token)
		ui.Info("Try it out with:  seedmancer schemas list")
		return nil
	case <-time.After(timeout):
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = server.Shutdown(shutdownCtx)
		cancel()
		spinner.Stop(false, "Timed out")
		return fmt.Errorf("timed out after %s waiting for browser sign-in - re-run seedmancer login", timeout)
	}
}

// warnIfEnvTokenShadows nudges the user when SEEDMANCER_API_TOKEN is set in
// the environment to a value that differs from the credentials we just
// saved. The credentials file now ranks above the env var in
// ResolveAPIToken, so the saved token will actually be used — but we still
// want the user to know about the mismatch so a future `unset` doesn't
// surprise them.
func warnIfEnvTokenShadows(saved string) {
	envTok := strings.TrimSpace(os.Getenv("SEEDMANCER_API_TOKEN"))
	if envTok == "" || envTok == strings.TrimSpace(saved) {
		return
	}
	ui.Warn("SEEDMANCER_API_TOKEN is set in your shell to a different value.")
	ui.Info("The newly-saved credentials file will be used instead. To remove the stale env var:")
	ui.Info("  unset SEEDMANCER_API_TOKEN")
}

func resolveDashboardURL(flag string) string {
	if v := strings.TrimSpace(flag); v != "" {
		return strings.TrimRight(v, "/")
	}
	// Allow reusing SEEDMANCER_API_URL for dev hosts that serve both the
	// dashboard and API from the same origin. Production runs the API on a
	// subdomain so we also allow SEEDMANCER_DASHBOARD_URL to differ.
	if v := strings.TrimSpace(utils.GetBaseURL()); v != "" && !strings.Contains(v, "api.seedmancer.dev") {
		return strings.TrimRight(v, "/")
	}
	return "https://seedmancer.dev"
}

func buildAuthURL(dashboard, code, callback string) string {
	u, err := url.Parse(dashboard)
	if err != nil {
		return dashboard
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/auth/cli"
	q := u.Query()
	q.Set("code", code)
	q.Set("callback", callback)
	u.RawQuery = q.Encode()
	return u.String()
}

func randomCode() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func openBrowser(target string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", target)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		cmd = exec.Command("xdg-open", target)
	}
	return cmd.Start()
}

const callbackPageTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{{title}}</title>
<meta name="viewport" content="width=device-width,initial-scale=1" />
<style>
  :root { color-scheme: dark; }
  body { margin: 0; background: #030712; color: #e5e7eb; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  main { min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; }
  .card { max-width: 440px; width: 100%; border: 1px solid #1f2937; background: rgba(17,24,39,0.6); border-radius: 16px; padding: 32px; text-align: center; }
  h1 { margin: 0 0 8px; font-size: 20px; color: #f3f4f6; }
  p { margin: 0; font-size: 14px; color: #9ca3af; line-height: 1.55; }
  .dot { width: 44px; height: 44px; border-radius: 999px; background: {{accent}}22; display: inline-flex; align-items: center; justify-content: center; margin-bottom: 16px; }
  .dot span { width: 12px; height: 12px; border-radius: 999px; background: {{accent}}; }
</style>
</head>
<body>
  <main>
    <div class="card">
      <div class="dot"><span></span></div>
      <h1>{{title}}</h1>
      <p>{{message}}</p>
    </div>
  </main>
</body>
</html>`

func renderCallback(w http.ResponseWriter, ok bool, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
	}
	title := "Seedmancer CLI signed in"
	accent := "#10b981"
	if !ok {
		title = "Seedmancer CLI sign-in failed"
		accent = "#ef4444"
	}
	page := callbackPageTemplate
	page = strings.ReplaceAll(page, "{{title}}", title)
	page = strings.ReplaceAll(page, "{{accent}}", accent)
	page = strings.ReplaceAll(page, "{{message}}", message)
	_, _ = w.Write([]byte(page))
}
