package gointerp

import (
	"fmt"
	"go/build"
	"strings"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

// Run interprets a Go source string (package main, stdlib only) via yaegi.
// outDir is injected as os.Args[1] inside the script, matching the same
// contract used by go run.
//
// Errors from the script (panics, os.Exit non-zero, compile errors) are
// returned as Go errors — the caller is responsible for cleaning up outDir.
func Run(src, outDir string) error {
	i := interp.New(interp.Options{
		GoPath: build.Default.GOPATH,
		// Inject the output directory as the first positional argument so
		// scripts can access it via os.Args[1], identical to the go-run path.
		Args: []string{"seedmancer-gen", outDir},
	})

	if err := i.Use(stdlib.Symbols); err != nil {
		return fmt.Errorf("loading stdlib into interpreter: %w", err)
	}

	if _, err := i.Eval(src); err != nil {
		// yaegi wraps panic messages in its own error type. Strip the
		// interpreter prefix to keep the message readable to agents.
		msg := err.Error()
		msg = strings.TrimPrefix(msg, "1:1: ")
		return fmt.Errorf("script error: %s", msg)
	}
	return nil
}
