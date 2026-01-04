package warpdb

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/Issaminu/warp-db/internal"
)

const (
	MagicNumber = "WARP"
	Version     = 1
)

// TODO: Add checksums for entire file (in Trailer section) and per-collection (in CollectionHeader)

// FileMetadata is the first 24 bytes of every .warpdb file
type FileMetadata struct {
	Magic           [6]byte  // "WARPDB"
	Version         uint32   // File format version
	Id              [16]byte // Unique identifier for this database file
	CollectionCount uint32   // Number of collections in this file
}

// CollectionHeader is at the start of each collection's data
type CollectionHeader struct {
	NameLength uint32
	Name       []byte
	EntryCount uint32          // Number of key-value pairs
	config     internal.Config // Configuration data for this collection
}

type WarpDB struct {
	path        string
	dbFile      *os.File
	metadata    FileMetadata
	collections []internal.Collection
}

func startDatabase(path string, globalConfig internal.Config) *WarpDB {

	databaseFile := getOrCreateDatabase(path)

	collections := parseDbFile(databaseFile)

	return &WarpDB{
		path:        path,
		dbFile:      databaseFile,
		collections: collections,
	}
}

func parseDbFile(dbFile *os.File) []internal.Collection {
	scanner := bufio.NewScanner(dbFile)
	var counter int
	var collectionName string

	var collections []internal.Collection
}

func getOrCreateDatabase(path string) *os.File {
	if path[len(path)-7:] != ".warpdb" {
		log.Fatal("database file path should end in '.warpdb'")
	}

	file, err := os.Open(path)

	if os.IsNotExist(err) {
		file, err = createDatabase(path)
	}

	if err != nil {
		log.Fatal(err)
	}

	return file
}

func createDatabase(path string) (*os.File, error) {
	log.Println("Database file not found. Creating it now...")

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	uuid, err := internal.GenerateUUIDv4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUIDv4: %v", err)
	}

	initialMetadata := FileMetadata{
		Magic:           [6]byte{'W', 'A', 'R', 'P', 'D', 'B'},
		Version:         Version,
		Id:              uuid,
		CollectionCount: 0,
	}

	_, err = file.Write(structToBytes(initialMetadata))
	if err != nil {
		return nil, fmt.Errorf("failed to write initial metadata: %v", err)
	}

	return file, nil
}

func main() {

}
