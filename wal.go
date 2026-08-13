package kvlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

const COMMIT_MARKER_RECORD_CHECKSUM = 0x67FBD83D // "KVLT-COMMIT-MARKER"

type WAL struct {
	db               *DB
	path             string
	file             *os.File
	collectedRecords map[Pgid]Record // Mapping Page ID to it's corresponding record. Only used temporarily to aggregate records that happen within a write operation, then flush at once
}

type Record struct {
	pgid        Pgid
	pageContent []byte
	checksum    uint64
}

func (wal *WAL) insertNodeRecord(node *Node) {
	buf := new(bytes.Buffer)
	encodeNode(node, buf)

	record := Record{
		pgid:        node.pgid,
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
	// wal.persistRecord(&record)
}

func (wal *WAL) insertMetaRecord(meta *Meta) {
	buf := new(bytes.Buffer)
	meta.encode(buf)

	record := Record{
		pgid:        0, // meta page
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
	// wal.persistRecord(&record)
}

func (wal *WAL) collectRecord(record *Record) {
	wal.collectedRecords[record.pgid] = *record
}

func (wal *WAL) readRecords() (*[]Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

	var records []Record
	for {
		record, err := decodeRecord(wal.file, wal.db.meta.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) { // read all records
				break
			}
			return nil, err
		}

		if !isRecordCommitMarker(record) {
			if err = record.Validate(); err != nil {
				return nil, err
			}
		}

		records = append(records, *record)
	}

	return &records, nil
}

func (wal *WAL) hasRecords() bool {
	fi, err := wal.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

// Truncate the WAL file.
// Important: only truncate the file after making sure that it's content has been ingested to the database
func (wal *WAL) clear() error {
	err := os.Truncate(wal.path, 0)
	return err
}

// Delete the WAL file
// Important: only delete the file when running db.Close()
func (wal *WAL) delete() error {
	// close the file before deletion
	err := wal.file.Close()
	if err != nil {
		return err
	}
	// delete the file from the disk
	err = os.Remove(wal.path)
	return err
}

func (wal *WAL) persistRecord(record *Record) error {
	pageSize := wal.db.meta.pageSize
	if len(record.pageContent) > int(pageSize) {
		return fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.pageContent), pageSize)
	}
	if len(record.pageContent) < int(pageSize) {
		padded := make([]byte, pageSize)
		copy(padded, record.pageContent)
		record.pageContent = padded
	}
	record.checksum = record.GenerateChecksum()
	return encodeRecord(wal.file, record, pageSize)
}

// Encodes a *Record instance into an io.Writer
func encodeRecord(w io.Writer, record *Record, pageSize int64) error {
	if err := binary.Write(w, binary.LittleEndian, record.pgid); err != nil {
		return fmt.Errorf("write record pgid: %w", err)
	}

	page := record.pageContent
	if len(page) > int(pageSize) {
		return fmt.Errorf("record page content exceeds page size (%d > %d)", len(page), pageSize)
	}
	if len(page) < int(pageSize) {
		padded := make([]byte, pageSize)
		copy(padded, page)
		page = padded
	}
	if err := writeFull(w, page); err != nil {
		return fmt.Errorf("write record pageContent: %w", err)
	}

	if err := binary.Write(w, binary.LittleEndian, record.checksum); err != nil {
		return fmt.Errorf("write record checksum: %w", err)
	}

	return nil
}

// Decodes a *Record instance from an io.Reader
func decodeRecord(r io.Reader, pageSize int64) (*Record, error) {
	record := &Record{}

	if err := binary.Read(r, binary.LittleEndian, &record.pgid); err != nil {
		return nil, fmt.Errorf("read record pgid: %w", err)
	}

	record.pageContent = make([]byte, pageSize)
	if _, err := io.ReadFull(r, record.pageContent); err != nil {
		return nil, fmt.Errorf("read record pageContent: %w", err)
	}

	if err := binary.Read(r, binary.LittleEndian, &record.checksum); err != nil {
		return nil, fmt.Errorf("read record checksum: %w", err)
	}

	return record, nil
}

func (wal *WAL) applyRecordToDatabase(record *Record, pageSize int64) error {
	offset := int64(record.pgid) * pageSize
	if _, err := wal.db.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := wal.db.file.Write(record.pageContent); err != nil {
		return err
	}
	// return db.file.Sync()
	return nil
}

func (record *Record) GenerateChecksum() uint64 {
	hashFunc := fnv.New64a()
	var pgidBuf [8]byte
	binary.LittleEndian.PutUint64(pgidBuf[:], uint64(record.pgid))
	hashFunc.Write(pgidBuf[:])
	hashFunc.Write(record.pageContent)
	return hashFunc.Sum64()
}

func (record *Record) Validate() error {
	if record.checksum != record.GenerateChecksum() {
		return ErrChecksum
	}
	return nil
}

func (wal *WAL) persistCollectedRecords() error {
	if wal.collectedRecords == nil {
		return nil
	}
	largeBuf := new(bytes.Buffer)
	recordBuffer := new(bytes.Buffer)

	for _, record := range wal.collectedRecords {
		pageSize := wal.db.meta.pageSize
		if len(record.pageContent) > int(pageSize) {
			return fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.pageContent), pageSize)
		}

		if len(record.pageContent) < int(pageSize) {
			padded := make([]byte, pageSize)
			copy(padded, record.pageContent)
			record.pageContent = padded
		}

		record.checksum = record.GenerateChecksum()
		encodeRecord(recordBuffer, &record, wal.db.meta.pageSize)

		// append to `largeBuf`
		_, err := largeBuf.ReadFrom(recordBuffer)
		if err != nil {
			return err
		}
	}

	wal.insertCommitMarker(largeBuf)

	if err := writeFull(wal.file, largeBuf.Bytes()); err != nil {
		return fmt.Errorf("persist multiple records: %w", err)
	}

	// reset for the next write batch
	wal.collectedRecords = make(map[Pgid]Record)

	return nil
}

func (wal *WAL) insertCommitMarker(buf *bytes.Buffer) error {
	recordBuffer := new(bytes.Buffer)
	commitMarker := &Record{
		pgid:        0,
		pageContent: []byte{},
		// pageContent: make([]byte, wal.db.meta.pageSize),
		checksum: COMMIT_MARKER_RECORD_CHECKSUM,
	}

	encodeRecord(recordBuffer, commitMarker, wal.db.meta.pageSize)

	_, err := buf.ReadFrom(recordBuffer)
	if err != nil {
		return err
	}

	return nil
}

func isRecordCommitMarker(record *Record) bool {
	return record.checksum == COMMIT_MARKER_RECORD_CHECKSUM
}
