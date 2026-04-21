package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CredentialsPath returns the on-disk location where `seedmancer login`
// stores the API token. Secrets live in their own file (never in
// seedmancer.yaml / ~/.seedmancer/config.yaml) so project config can be
// checked into source control without leaking tokens.
func CredentialsPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %v", err)
	}
	return filepath.Join(homeDir, ".seedmancer", "credentials"), nil
}

// LoadAPICredentials reads the API token from the credentials file. A
// missing file is not an error — callers fall back to flag / env / legacy
// config and only surface ErrMissingAPIToken when every source is empty.
func LoadAPICredentials() (string, error) {
	path, err := CredentialsPath()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("reading %s: %v", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}

// SaveAPICredentials writes the API token to ~/.seedmancer/credentials with
// 0600 perms so only the current user can read it. The parent directory is
// created with 0700 for the same reason.
func SaveAPICredentials(token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("refusing to save empty token")
	}
	path, err := CredentialsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("creating %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
		return fmt.Errorf("writing %s: %v", path, err)
	}
	return nil
}

// ClearAPICredentials removes the credentials file if it exists. Used by
// `seedmancer logout` (and test cleanup). Silently succeeds when the file is
// already absent so callers can treat logout as idempotent.
func ClearAPICredentials() error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s: %v", path, err)
	}
	return nil
}
