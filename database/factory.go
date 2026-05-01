package db

import (
	"fmt"
	"net/url"
	"strings"
)

// NewManager parses rawDSN, selects the right DatabaseManager implementation,
// and returns the normalized DSN that should be passed to ConnectWithDSN.
//
// Supported schemes:
//
//	postgres:// / postgresql://  → PostgresManager
//	mysql://                     → MySQLManager
func NewManager(rawDSN string) (DatabaseManager, string, error) {
	normalized, scheme, err := normalizeDSN(rawDSN)
	if err != nil {
		return nil, "", err
	}
	switch scheme {
	case "postgres":
		return &PostgresManager{}, normalized, nil
	case "mysql":
		return &MySQLManager{}, normalized, nil
	default:
		return nil, "", fmt.Errorf("unsupported database scheme %q (supported: postgres, mysql)", scheme)
	}
}

// normalizeDSN applies scheme-specific fixups and returns (normalizedDSN, scheme, err).
//
// Postgres fixups:
//   - "postgresql://" → "postgres://"
//   - appends "?sslmode=disable" when host is local and sslmode not set
//
// MySQL fixups:
//   - "mysql://user:pass@host:port/db" → "user:pass@tcp(host:port)/db?parseTime=true&multiStatements=true"
func normalizeDSN(rawDSN string) (string, string, error) {
	u, err := url.Parse(rawDSN)
	if err != nil {
		return "", "", fmt.Errorf("parsing database URL: %v", err)
	}

	switch u.Scheme {
	case "postgresql":
		rawDSN = "postgres" + rawDSN[len("postgresql"):]
		if !strings.Contains(rawDSN, "sslmode=") {
			rawDSN = appendQuery(rawDSN, "sslmode=disable")
		}
		return rawDSN, "postgres", nil

	case "postgres":
		if !strings.Contains(rawDSN, "sslmode=") {
			rawDSN = appendQuery(rawDSN, "sslmode=disable")
		}
		return rawDSN, "postgres", nil

	case "mysql":
		native, err := mysqlURLToNative(u)
		if err != nil {
			return "", "", err
		}
		return native, "mysql", nil

	default:
		// Return as-is with whatever scheme was found so the caller can
		// produce a meaningful "unsupported scheme" error.
		return rawDSN, u.Scheme, nil
	}
}

// mysqlURLToNative converts a standard mysql:// URL to the native DSN format
// expected by github.com/go-sql-driver/mysql:
//
//	user:pass@tcp(host:port)/dbname?params
func mysqlURLToNative(u *url.URL) (string, error) {
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "3306"
	}

	// Credentials
	var userInfo string
	if u.User != nil {
		pass, _ := u.User.Password()
		userInfo = u.User.Username() + ":" + pass + "@"
	}

	// Database name (strip leading slash)
	dbName := strings.TrimPrefix(u.Path, "/")

	// Merge caller-supplied query params with required defaults
	params := u.Query()
	if params.Get("parseTime") == "" {
		params.Set("parseTime", "true")
	}
	if params.Get("multiStatements") == "" {
		params.Set("multiStatements", "true")
	}

	dsn := fmt.Sprintf("%stcp(%s:%s)/%s?%s", userInfo, host, port, dbName, params.Encode())
	return dsn, nil
}

// appendQuery adds a key=value pair to a DSN URL string.
func appendQuery(dsn, kv string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + kv
	}
	return dsn + "?" + kv
}
