package snapshot

import (
	db "reseeder/database"
	"fmt"
)

func CreateWithDSN(dbType, dsn, name string) error {
	var manager interface {
		ConnectWithDSN(string) error
		CreateSnapshot(string) error
	}

	switch dbType {
	case "postgres":
		manager = &db.PostgresManager{}
	default:
		return fmt.Errorf("unsupported database type: %s", dbType)
	}

	if err := manager.ConnectWithDSN(dsn); err != nil {
		return fmt.Errorf("connection error: %v", err)
	}

	if err := manager.CreateSnapshot(name); err != nil {
		return fmt.Errorf("snapshot creation error: %v", err)
	}

	return nil
}

func Restore(dbType string, snapshotName string) error {
	var manager interface {
		ConnectWithDSN(string) error
		RestoreSnapshot(string) error
	}

	switch dbType {
	case "postgres":
		manager = &db.PostgresManager{}
	default:
		return fmt.Errorf("unsupported database type: %s", dbType)
	}

	dsn := "postgres://postgres:postgres@127.0.0.1:54322/postgres?sslmode=disable"
	if err := manager.ConnectWithDSN(dsn); err != nil {
		return fmt.Errorf("connection error: %v", err)
	}

	if err := manager.RestoreSnapshot(snapshotName); err != nil {
		return fmt.Errorf("restore error: %v", err)
	}

	return nil
}