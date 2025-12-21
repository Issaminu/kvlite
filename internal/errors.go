package internal

import "errors"

var (
	ErrKeyNotFound             = errors.New("key not found")
	ErrChecksumCompaisonFailed = errors.New("checksum compairson failed")
	ErrDatabaseClosed          = errors.New("database is closed")
	ErrCorruptedLog            = errors.New("log file corrupted")
	ErrDiskFull                = errors.New("disk full")
	ErrInvalidKey              = errors.New("key cannot be empty")
)
