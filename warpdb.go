package warpdb

import (
	"bytes"
	"encoding/binary"
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

// FileMetadata is the first 30 bytes of every .warpdb file
type FileMetadata struct {
	Magic           [6]byte  // "WARPDB"
	Version         uint32   // File format version
	Id              [16]byte // Unique identifier for this database file
	CollectionCount uint32   // Number of collections in this file
}

type WarpDB struct {
	Path        string
	dbFile      *os.File
	Metadata    FileMetadata
	Collections map[string]*internal.Collection
}

func StartDatabase(path string, globalConfig internal.Config) *WarpDB {

	databaseFile := getOrCreateDatabase(path)

	collections := parseDbFile(databaseFile)

	return &WarpDB{
		Path:        path,
		dbFile:      databaseFile,
		Collections: collections,
		Metadata:    NewFileMetadata(),
	}
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

func NewFileMetadata() FileMetadata {
	uuid, err := internal.GenerateUUIDv4()
	if err != nil {
		log.Fatal("failed to generate UUIDv4 for new database file:", err)
	}

	return FileMetadata{
		Magic:           [6]byte{'W', 'A', 'R', 'P', 'D', 'B'},
		Version:         Version,
		Id:              uuid,
		CollectionCount: 1, // Starts with default collection
	}
}

func createDatabase(path string) (*os.File, error) {
	log.Println("Database file not found. Creating it now...")

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	initialFileMetadata := NewFileMetadata()

	encodedFileMetadata, err := internal.StructToBytes(initialFileMetadata, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to encode initial metadata: %v", err)
	}

	_, err = file.Write(encodedFileMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to write file metadata: %v", err)
	}

	defaultCollection := internal.NewCollection("default", internal.DefaultConfig())

	// TODO: To be removed later
	defaultCollection.Set("test", "123")

	encodedDefaultCollectionData, err := encodeCollection(defaultCollection)
	if err != nil {
		return nil, fmt.Errorf("failed to encode default collection data: %v", err)
	}

	_, err = file.Write(encodedDefaultCollectionData)
	if err != nil {
		return nil, fmt.Errorf("failed to write default collection data: %v", err)
	}

	return file, nil
}

func (db *WarpDB) Close() error {
	return db.dbFile.Close()
}

func (db *WarpDB) GetCollection(name string) (*internal.Collection, error) {
	collection, found := db.Collections[name]
	if !found {
		return nil, fmt.Errorf("collection '%s' not found", name)
	}

	return collection, nil
}
func (db *WarpDB) CreateCollection(name string, config internal.Config) (*internal.Collection, error) {
	_, found := db.Collections[name]
	if found {
		return nil, fmt.Errorf("collection '%s' already exists", name)
	}

	newCollection := internal.NewCollection(name, config)
	db.Collections[name] = newCollection

	db.Metadata.CollectionCount++

	return newCollection, nil
}

func (db *WarpDB) DeleteCollection(name string) error {
	_, found := db.Collections[name]
	if !found {
		return fmt.Errorf("collection '%s' not found", name)
	}

	delete(db.Collections, name)
	db.Metadata.CollectionCount--

	return nil
}

func (db *WarpDB) ListCollections() []string {
	collectionNames := make([]string, 0, len(db.Collections))
	for name := range db.Collections {
		collectionNames = append(collectionNames, name)
	}
	return collectionNames
}

func (db *WarpDB) Set(collectionName, key, value string) error {
	collection, found := db.Collections[collectionName]
	if !found {
		return fmt.Errorf("collection '%s' not found", collectionName)
	}

	_, err := collection.Set(key, value)
	return err
}

func (db *WarpDB) Get(collectionName, key string) (string, error) {
	collection, found := db.Collections[collectionName]
	if !found {
		return "", fmt.Errorf("collection '%s' not found", collectionName)
	}

	return collection.Get(key)
}
func (db *WarpDB) Delete(collectionName, key string) error {
	collection, found := db.Collections[collectionName]
	if !found {
		return fmt.Errorf("collection '%s' not found", collectionName)
	}

	_, err := collection.Delete(key)
	return err
}

func encodeCollection(collection *internal.Collection) ([]byte, error) {
	var buf bytes.Buffer

	metadata := collection.Metadata
	store := collection.GetStore()

	// Write nameLength
	nameLength := metadata.NameLength
	nameBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(nameBytes, nameLength)
	buf.Write(nameBytes)

	// Write name
	buf.Write(metadata.Name)

	// Write entryCount
	entryCount := metadata.EntryCount
	entryCountBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(entryCountBytes, entryCount)
	buf.Write(entryCountBytes)

	// Write config
	configBytes, err := internal.StructToBytes(metadata.Config, nil)
	if err != nil {
		return nil, err
	}
	buf.Write(configBytes)

	// Write each key-value pair
	for key, val := range store {
		// Write key length
		keyLength := uint32(len(key))
		keyLengthBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLengthBytes, keyLength)
		buf.Write(keyLengthBytes)

		// Write key
		buf.WriteString(key)

		// Write value length
		valLength := uint32(len(val))
		valLengthBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(valLengthBytes, valLength)
		buf.Write(valLengthBytes)

		// Write value
		buf.WriteString(val)
	}

	return buf.Bytes(), nil
}

func parseDbFile(dbFile *os.File) map[string]*internal.Collection {
	collections := make(map[string]*internal.Collection)

	// Read the entire file content
	fileInfo, err := dbFile.Stat()
	if err != nil {
		log.Printf("failed to get file info: %v", err)
		return collections
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return collections
	}

	fileData := make([]byte, fileSize)
	_, err = dbFile.Seek(0, 0) // Reset to beginning
	if err != nil {
		log.Printf("failed to seek to beginning: %v", err)
		return collections
	}

	_, err = dbFile.Read(fileData)
	if err != nil {
		log.Printf("failed to read file: %v", err)
		return collections
	}

	// Step 1: Read FileMetadata (first 30 bytes)
	var metadata FileMetadata
	if len(fileData) < 30 {
		log.Println("file too small to contain metadata")
		return collections
	}

	reader := bytes.NewReader(fileData)

	err = binary.Read(reader, binary.LittleEndian, &metadata)
	if err != nil {
		log.Printf("failed to decode file metadata: %v", err)
		return collections
	}

	// Validate magic number
	if string(metadata.Magic[:]) != "WARPDB" {
		log.Println("invalid magic number in database file")
		return collections
	}

	// Step 2: Read each collection
	for i := uint32(0); i < metadata.CollectionCount; i++ {
		// Step 2a: Read collection metadata
		var nameLength uint32
		err = binary.Read(reader, binary.LittleEndian, &nameLength)
		if err != nil {
			log.Printf("failed to read nameLength for collection %d: %v", i, err)
			break
		}

		// Read name bytes
		nameBytes := make([]byte, nameLength)
		_, err = reader.Read(nameBytes)
		if err != nil {
			log.Printf("failed to read name for collection %d: %v", i, err)
			break
		}
		collectionName := string(nameBytes)

		// Read entryCount
		var entryCount uint32
		err = binary.Read(reader, binary.LittleEndian, &entryCount)
		if err != nil {
			log.Printf("failed to read entryCount for collection %d: %v", i, err)
			break
		}

		// Read Config struct
		var config internal.Config
		err = binary.Read(reader, binary.LittleEndian, &config)
		if err != nil {
			log.Printf("failed to decode config for collection %d: %v", i, err)
			break
		}

		// Create collection with metadata
		collection := internal.NewCollection(collectionName, config)

		// Step 2b: Read collection items one by one
		for j := uint32(0); j < entryCount; j++ {
			// Read key length
			var keyLength uint32
			err = binary.Read(reader, binary.LittleEndian, &keyLength)
			if err != nil {
				log.Printf("failed to read key length for entry %d in collection %d: %v", j, i, err)
				break
			}

			// Read key
			keyBytes := make([]byte, keyLength)
			_, err = reader.Read(keyBytes)
			if err != nil {
				log.Printf("failed to read key for entry %d in collection %d: %v", j, i, err)
				break
			}
			key := string(keyBytes)

			// Read value length
			var valueLength uint32
			err = binary.Read(reader, binary.LittleEndian, &valueLength)
			if err != nil {
				log.Printf("failed to read value length for entry %d in collection %d: %v", j, i, err)
				break
			}

			// Read value
			valueBytes := make([]byte, valueLength)
			_, err = reader.Read(valueBytes)
			if err != nil {
				log.Printf("failed to read value for entry %d in collection %d: %v", j, i, err)
				break
			}
			value := string(valueBytes)

			// Set the key-value pair in the collection
			_, err = collection.Set(key, value)
			if err != nil {
				log.Printf("failed to set key-value pair: %v", err)
			}
		}

		collections[collectionName] = collection
	}

	return collections
}
