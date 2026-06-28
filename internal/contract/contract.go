// Package contract reads and writes the optional contract.yaml that can sit
// alongside a Seedmancer state (scenario). A contract describes what a state
// means — its purpose, the named data handles it provides, and the
// invariants it must satisfy — rather than the raw rows it contains.
//
// The contract lives at <scenarioDir>/contract.yaml. The Playwright
// integration reads `provides` to expose named handles via seedmancer.get();
// the CLI surfaces `purpose` in check output.
package contract

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// FileName is the on-disk name of a state contract.
const FileName = "contract.yaml"

// Contract is the parsed contract.yaml. Values in Provides are intentionally
// loosely typed (string/bool/number) so the format can carry plain values and
// `*Env` references without a rigid schema.
type Contract struct {
	State    string                            `yaml:"state,omitempty" json:"state,omitempty"`
	Purpose  string                            `yaml:"purpose,omitempty" json:"purpose,omitempty"`
	Provides map[string]map[string]interface{} `yaml:"provides,omitempty" json:"provides,omitempty"`
	MustHave []string                          `yaml:"mustHave,omitempty" json:"mustHave,omitempty"`
}

// Path returns the contract path for a given scenario directory.
func Path(scenarioDir string) string {
	return filepath.Join(scenarioDir, FileName)
}

// Load reads the contract for a scenario directory. The bool return is false
// (with a nil error) when no contract exists — absence is not an error.
func Load(scenarioDir string) (*Contract, bool, error) {
	path := Path(scenarioDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var c Contract
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, false, err
	}
	return &c, true, nil
}

// Write persists a contract to a scenario directory, creating the directory
// if needed. It writes atomically (temp file + rename).
func Write(scenarioDir string, c Contract) error {
	if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
		return err
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	path := Path(scenarioDir)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
