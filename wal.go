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

const (
	recordTypeData   uint8 = 0
	recordTypeMeta   uint8 = 1
	recordTypeCommit uint8 = 2 // a commit marker: ends one transaction, no content
)

// recordHeaderSize is the fixed header size: type(1) + pgid(8) + txid(8) + content_size(4)
const recordHeaderSize = 1 + 8 + 8 + 4

type WAL struct {
	db                       *DB
	path                     string
	file                     *os.File
	checkpointThresholdBytes uint32
	bytesSinceCheckpoint     uint32
	collectedRecords         map[Pgid]Record // Mapping Page ID to it's corresponding record. Only used temporarily within the current transaction to aggregate records that happen within a write operation, then flush at once
	overlay                  map[Pgid]Record // Mapping that committed-but-not-yet-checkpointed pages, it's content comes from collectedRecords. This mapping lives beyond a single transaction
	nextTxid                 Txid            // sequence number stamped on the next committed transaction
}

// RecordHeader is the fixed-size head of every WAL record.
type RecordHeader struct {
	recordType  uint8
	pgid        Pgid
	txid        Txid
	contentSize uint32 // length of `Record.pageContent`. In the WAL file, it acts as pageContent's prefixed length
}

func (h RecordHeader) encode() []byte {
	b := make([]byte, 0, recordHeaderSize)
	b = append(b, h.recordType)
	b = binary.LittleEndian.AppendUint64(b, uint64(h.pgid))
	b = binary.LittleEndian.AppendUint64(b, uint64(h.txid))
	b = binary.LittleEndian.AppendUint32(b, h.contentSize)
	return b
}

// decodeRecordHeader parses a header from its raw bytes.
func decodeRecordHeader(raw []byte) RecordHeader {
	return RecordHeader{
		recordType:  raw[0],
		pgid:        Pgid(binary.LittleEndian.Uint64(raw[1:9])),
		txid:        Txid(binary.LittleEndian.Uint64(raw[9:17])),
		contentSize: binary.LittleEndian.Uint32(raw[17:21]),
	}
}

type Record struct {
	header      RecordHeader
	pageContent []byte // actual content of the `Record`. We can identify it's length by a prefixed attribute `header.contentSize`
	checksum    uint64
}

func (wal *WAL) insertNodeRecord(node *Node) {
	buf := new(bytes.Buffer)
	encodeNode(node, buf)

	record := Record{
		header:      RecordHeader{recordType: recordTypeData, pgid: node.pgid},
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) insertMetaRecord(meta *Meta) {
	meta.checksum = meta.GenerateChecksum() // meta is mutated each Put so we should refresh it's checksum before encoding
	buf := new(bytes.Buffer)
	meta.encode(buf)

	record := Record{
		header:      RecordHeader{recordType: recordTypeMeta, pgid: metaPgid},
		pageContent: buf.Bytes(),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) collectRecord(record *Record) {
	wal.collectedRecords[record.header.pgid] = *record
}

func (wal *WAL) readRecords() (*[]Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

	var records []Record
	for {
		record, err := decodeRecord(wal.file, wal.db.meta.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrChecksum) {
				break
			}
			return nil, err
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
func (wal *WAL) Truncate() error {
	if err := os.Truncate(wal.path, 0); err != nil {
		return err
	}
	// rewind the handle so writes after a recovery start at the beginning
	_, err := wal.file.Seek(0, io.SeekStart)
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

// Encodes a *Record instance into an io.Writer
func encodeRecord(w io.Writer, record *Record, pageSize int64) error {
	if int64(len(record.pageContent)) > pageSize {
		return fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.pageContent), pageSize)
	}

	record.header.contentSize = uint32(len(record.pageContent))
	header := record.header.encode()

	checksum := computeRecordChecksum(header, record.pageContent)

	if err := writeFull(w, header); err != nil {
		return fmt.Errorf("write record header: %w", err)
	}
	// no need to use `writeLengthPrefixedBytes` here since it's implicitely happening:
	// the 4 bytes that sit before pageContent is it's prefixed length (`RecordHeader.contentSize`)
	if err := writeFull(w, record.pageContent); err != nil {
		return fmt.Errorf("write record pageContent: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("write record checksum: %w", err)
	}

	return nil
}

// Decodes a *Record instance from an io.Reader
func decodeRecord(r io.Reader, pageSize int64) (*Record, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err // io.EOF at a clean boundary; io.ErrUnexpectedEOF on a torn tail
	}

	record := &Record{header: decodeRecordHeader(header)}
	if int64(record.header.contentSize) > pageSize {
		// A torn tail can leave a bogus length prefix. Treat it as an integrity
		// failure (ErrChecksum) so readRecords stops at this record instead of
		// failing the whole open. A well-formed record can never exceed a page.
		return nil, fmt.Errorf("record content_size %d exceeds page size %d: %w", record.header.contentSize, pageSize, ErrChecksum)
	}

	record.pageContent = make([]byte, record.header.contentSize)
	if _, err := io.ReadFull(r, record.pageContent); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.LittleEndian, &record.checksum); err != nil {
		return nil, err
	}
	if computeRecordChecksum(header, record.pageContent) != record.checksum {
		return nil, ErrChecksum
	}

	return record, nil
}

func (wal *WAL) applyRecordToDatabase(record *Record, pageSize int64) error {
	offset := int64(record.header.pgid) * pageSize
	if _, err := wal.db.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	// content is stored unpadded in the WAL; pad it to a full page for the main file
	page := record.pageContent
	if int64(len(page)) < pageSize {
		padded := make([]byte, pageSize)
		copy(padded, page)
		page = padded
	}
	if _, err := wal.db.file.Write(page); err != nil {
		return err
	}
	return nil
}

func computeRecordChecksum(header, content []byte) uint64 {
	hashFunc := fnv.New64a()
	hashFunc.Write(header)
	hashFunc.Write(content)
	return hashFunc.Sum64()
}

func (wal *WAL) persistCollectedRecords() error {
	if len(wal.collectedRecords) == 0 {
		return nil
	}

	txid := wal.nextTxid
	wal.nextTxid++

	largeBuf := new(bytes.Buffer)
	recordBuffer := new(bytes.Buffer)

	for pgid, record := range wal.collectedRecords {
		record.header.txid = txid

		if err := encodeRecord(recordBuffer, &record, wal.db.meta.pageSize); err != nil {
			return err
		}

		// append to `largeBuf`
		_, err := largeBuf.ReadFrom(recordBuffer)
		if err != nil {
			return err
		}

		// Write the stamped copy back into the map
		wal.collectedRecords[pgid] = record
	}

	if err := wal.insertCommitMarker(largeBuf, txid); err != nil {
		return err
	}

	walBytes := uint32(largeBuf.Len())

	if err := writeFull(wal.file, largeBuf.Bytes()); err != nil {
		return fmt.Errorf("persist multiple records: %w", err)
	}
	wal.bytesSinceCheckpoint += walBytes

	// transaction complete, reset collectedRecords

	for key := range wal.collectedRecords {
		wal.overlay[key] = wal.collectedRecords[key]
	}
	clear(wal.collectedRecords)

	// check if it's time to fsync the WAL
	if wal.db.options.synchronous == SYNCHRONOUS_FULL {
		err := wal.file.Sync()
		if err != nil {
			return err
		}
	}

	// check if we should checkpoint into the DB file
	if wal.reachedCheckpointThreshold() {
		if err := wal.checkpoint(); err != nil {
			return err
		}
	}
	return nil
}

func (wal *WAL) insertCommitMarker(buf *bytes.Buffer, txid Txid) error {
	recordBuffer := new(bytes.Buffer)
	commitMarker := &Record{
		header:      RecordHeader{recordType: recordTypeCommit, pgid: 0, txid: txid},
		pageContent: nil,
	}

	if err := encodeRecord(recordBuffer, commitMarker, wal.db.meta.pageSize); err != nil {
		return fmt.Errorf("encode commit marker: %w", err)
	}

	_, err := buf.ReadFrom(recordBuffer)
	if err != nil {
		return err
	}

	return nil
}

func isRecordCommitMarker(record *Record) bool {
	return record.header.recordType == recordTypeCommit
}

func (wal *WAL) reachedCheckpointThreshold() bool {
	return wal.bytesSinceCheckpoint >= wal.checkpointThresholdBytes
}

func (record *Record) toNode() (*Node, error) {
	if record.pageContent == nil {
		return nil, fmt.Errorf("record has no page content")
	}

	node, err := decodeNode(bytes.NewReader(record.pageContent))

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}

// checkpoint() flushes the WAL overlay to the main database file and resets the WAL state.
// It ensures durability by syncing both the WAL and database files, then truncates the WAL
// and clears the in-memory overlay to prepare for new transactions.
func (wal *WAL) checkpoint() error {
	if len(wal.overlay) == 0 {
		return nil
	}

	// make WAL durable
	if err := wal.file.Sync(); err != nil {
		return err
	}

	// drain in-memory wal.overlay to main
	for _, record := range wal.overlay {
		if err := wal.applyRecordToDatabase(&record, wal.db.meta.pageSize); err != nil {
			return err
		}
	}

	// fsync main db file after applying the records to it
	if err := wal.db.file.Sync(); err != nil {
		return err
	}

	//reset WAL
	if err := wal.Truncate(); err != nil {
		return err
	}
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	return nil
}
