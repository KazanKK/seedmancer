// Package usage aggregates the per-test usage events written by the
// Seedmancer Playwright integration into a queryable picture of which
// Seedmancer states are used by which tests.
//
// The integration writes one uniquely-named JSON file per test+state+project
// under <storagePath>/.usage-events/. Unique filenames mean parallel test
// workers never race on the same file. This package reads those events and
// folds them into per-state records, deduplicating by file+title+project and
// keeping the most recent sighting.
//
// The aggregated view is also persisted to <storagePath>/state-usage.json so
// it can be inspected directly, but the event files remain the source of
// truth — state-usage.json is a derived cache.
package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	eventsDirName     = ".usage-events"
	stateUsageName    = "state-usage.json"
)

// Event is a single usage sighting written by the Playwright integration.
type Event struct {
	State      string    `json:"state"`
	File       string    `json:"file"`
	Title      string    `json:"title"`
	Project    string    `json:"project,omitempty"`
	ResetMode  string    `json:"resetMode,omitempty"`
	LastSeenAt time.Time `json:"lastSeenAt"`
	TestHash   string    `json:"testHash,omitempty"`
}

// Ref is one test that uses a state, in the aggregated view.
type Ref struct {
	File       string    `json:"file"`
	Title      string    `json:"title"`
	Project    string    `json:"project,omitempty"`
	ResetMode  string    `json:"resetMode,omitempty"`
	LastSeenAt time.Time `json:"lastSeenAt"`
	TestHash   string    `json:"testHash,omitempty"`
}

// StateUsage is the aggregated usage for a single state.
type StateUsage struct {
	State      string    `json:"state"`
	UsedBy     []Ref     `json:"usedBy"`
	LastUsedAt time.Time `json:"lastUsedAt,omitempty"`
}

// Aggregate is the persisted shape of state-usage.json.
type Aggregate struct {
	States map[string]*StateUsage `json:"states"`
}

// EventsDir returns the directory holding raw usage events.
func EventsDir(projectRoot, storagePath string) string {
	return filepath.Join(projectRoot, storagePath, eventsDirName)
}

// StateUsagePath returns the path of the aggregated state-usage.json cache.
func StateUsagePath(projectRoot, storagePath string) string {
	return filepath.Join(projectRoot, storagePath, stateUsageName)
}

func dedupeKey(r Ref) string {
	return r.File + "\x00" + r.Title + "\x00" + r.Project
}

// Load reads every usage event under EventsDir and folds it into a per-state
// aggregate. A missing events directory is not an error — it returns an empty
// aggregate. Unreadable or malformed individual event files are skipped so a
// single bad file never hides the rest.
func Load(projectRoot, storagePath string) (Aggregate, error) {
	agg := Aggregate{States: map[string]*StateUsage{}}

	dir := EventsDir(projectRoot, storagePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return agg, nil
		}
		return agg, err
	}

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		raw, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		if rerr != nil {
			continue
		}
		var ev Event
		if json.Unmarshal(raw, &ev) != nil || ev.State == "" {
			continue
		}

		su := agg.States[ev.State]
		if su == nil {
			su = &StateUsage{State: ev.State}
			agg.States[ev.State] = su
		}

		ref := Ref{
			File:       ev.File,
			Title:      ev.Title,
			Project:    ev.Project,
			ResetMode:  ev.ResetMode,
			LastSeenAt: ev.LastSeenAt,
			TestHash:   ev.TestHash,
		}

		// Deduplicate by file+title+project, keeping the newest sighting.
		replaced := false
		for i := range su.UsedBy {
			if dedupeKey(su.UsedBy[i]) == dedupeKey(ref) {
				if ref.LastSeenAt.After(su.UsedBy[i].LastSeenAt) {
					su.UsedBy[i] = ref
				}
				replaced = true
				break
			}
		}
		if !replaced {
			su.UsedBy = append(su.UsedBy, ref)
		}
		if ev.LastSeenAt.After(su.LastUsedAt) {
			su.LastUsedAt = ev.LastSeenAt
		}
	}

	// Stable ordering for deterministic output.
	for _, su := range agg.States {
		sort.Slice(su.UsedBy, func(i, j int) bool {
			if su.UsedBy[i].File != su.UsedBy[j].File {
				return su.UsedBy[i].File < su.UsedBy[j].File
			}
			return su.UsedBy[i].Title < su.UsedBy[j].Title
		})
	}

	return agg, nil
}

// Persist writes the aggregate to state-usage.json (best-effort cache).
func Persist(projectRoot, storagePath string, agg Aggregate) error {
	path := StateUsagePath(projectRoot, storagePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(agg, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
