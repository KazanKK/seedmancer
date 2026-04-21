package ui

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"
)

var (
	debugMode bool
	noColor   bool
)

func init() {
	noColor = !term.IsTerminal(int(os.Stdout.Fd()))
}

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	dim    = "\033[2m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	gray   = "\033[90m"
)

func color(code, text string) string {
	if noColor {
		return text
	}
	return code + text + reset
}

func SetDebug(enabled bool) { debugMode = enabled }
func IsDebug() bool         { return debugMode }

func Step(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", color(cyan, "→"), msg)
}

func Success(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", color(green, "✓"), msg)
}

func Warn(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", color(yellow, "⚠"), msg)
}

func Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", color(red, "✗"), msg)
}

func Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "  %s\n", msg)
}

func Debug(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(os.Stderr, "  %s\n", color(gray, msg))
}

func Title(text string) {
	fmt.Fprintf(os.Stderr, "\n%s\n%s\n", color(bold, text), color(dim, strings.Repeat("─", len(text))))
}

func KeyValue(key, value string) {
	fmt.Fprintf(os.Stderr, "  %s %s\n", color(dim, key), value)
}

// PrintLoginHint renders the "how to sign in" guide shown whenever a command
// cannot resolve an API token. Keeping this in one place guarantees every
// command (generate, sync, fetch, list, schemas) shows the exact same copy
// and highlights `seedmancer login` as the recommended path.
func PrintLoginHint() {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "%s %s\n", color(red, "✗"), color(bold, "You're not signed in to Seedmancer."))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", color(dim, "Sign in with your browser (recommended):"))
	fmt.Fprintf(os.Stderr, "    %s\n", color(green, "seedmancer login"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", color(dim, "Or provide a token manually:"))
	fmt.Fprintf(os.Stderr, "    %s\n", color(cyan, "--token <API_TOKEN>"))
	fmt.Fprintf(os.Stderr, "    %s\n", color(cyan, "export SEEDMANCER_API_TOKEN=<API_TOKEN>"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s %s\n", color(dim, "Manage tokens:"), "https://seedmancer.dev/dashboard/settings")
	fmt.Fprintln(os.Stderr)
}

// IsTerminal returns true when stderr is a TTY (interactive session).
func IsTerminal() bool { return !noColor }

// Confirm prompts the user with a yes/no question. Returns true for y/Y/yes.
// In non-TTY environments, returns the defaultVal without prompting.
func Confirm(prompt string, defaultVal bool) bool {
	hint := "[y/N]"
	if defaultVal {
		hint = "[Y/n]"
	}
	fmt.Fprintf(os.Stderr, "%s %s %s ", color(yellow, "?"), prompt, color(dim, hint))

	if noColor {
		// non-interactive: use default
		if defaultVal {
			fmt.Fprintln(os.Stderr, "y")
		} else {
			fmt.Fprintln(os.Stderr, "n")
		}
		return defaultVal
	}

	var answer string
	fmt.Scanln(&answer)
	answer = strings.ToLower(strings.TrimSpace(answer))
	if answer == "" {
		return defaultVal
	}
	return answer == "y" || answer == "yes"
}

// Spinner provides animated progress for long-running operations.
type Spinner struct {
	message string
	done    chan struct{}
	mu      sync.Mutex
	stopped bool
}

func StartSpinner(message string) *Spinner {
	s := &Spinner{
		message: message,
		done:    make(chan struct{}),
	}
	if noColor {
		fmt.Fprintf(os.Stderr, "%s %s...\n", color(cyan, "→"), message)
		return s
	}
	go s.run()
	return s
}

func (s *Spinner) run() {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	i := 0
	for {
		select {
		case <-s.done:
			return
		default:
			fmt.Fprintf(os.Stderr, "\r%s %s", color(cyan, frames[i%len(frames)]), s.message)
			i++
			time.Sleep(80 * time.Millisecond)
		}
	}
}

func (s *Spinner) UpdateMessage(msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.message = msg
}

func (s *Spinner) Stop(success bool, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	close(s.done)
	if noColor {
		if success {
			fmt.Fprintf(os.Stderr, "✓ %s\n", message)
		} else {
			fmt.Fprintf(os.Stderr, "✗ %s\n", message)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "\r\033[2K")
	if success {
		fmt.Fprintf(os.Stderr, "%s %s\n", color(green, "✓"), message)
	} else {
		fmt.Fprintf(os.Stderr, "%s %s\n", color(red, "✗"), message)
	}
}
