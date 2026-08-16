package kvlite

import (
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

	recordTypeSize          = 1 // A record type uses one byte.
	recordTransactionIDSize = 8 // A transaction ID uses one uint64 value.
	recordContentLengthSize = 4 // A content length uses one uint32 value.
	recordChecksumSize      = 8 // An FNV-1a checksum uses one uint64 value.
	recordHeaderSize        = recordTypeSize + txidEncodedSize + recordTransactionIDSize + recordContentLengthSize
)

type WAL struct {
	db                       *DB
	path                     string
	file                     *os.File
	checkpointThresholdBytes uint32
	bytesSinceCheckpoint     uint32
	collectedRecords         map[Pgid]Record // Mapping Page ID to it's corresponding record. Only used temporarily within the current transaction to aggregate records that happen within a write operation, then flush at once
	overlay                  map[Pgid]Record // Mapping that committed-but-not-yet-checkpointed pages, it's content comes from collectedRecords. This mapping lives beyond a single transaction
	nextTxid                 Txid            // sequence number stamped on the next committed transaction
	hasUnsyncedWrites        bool            // true when WAL bytes were appended after the last successful sync
	syncFile                 func() error    // syncFile is an hook used exclusively for tests to determine deterministic sync failures and call counts.
}

// RecordHeader is the fixed-size head of every WAL record.
type RecordHeader struct {
	recordType uint8
	pgid       Pgid
	txid       Txid
}

type Record struct {
	header      RecordHeader
	pageContent []byte
}

func (wal *WAL) insertNodeRecord(node *Node) {
	record := Record{
		header:      RecordHeader{recordType: recordTypeData, pgid: node.pgid},
		pageContent: encodeNode(node),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) insertMetaRecord(meta *Meta) {
	meta.checksum = meta.GenerateChecksum() // meta is mutated each Put so we should refresh it's checksum before encoding

	record := Record{
		header:      RecordHeader{recordType: recordTypeMeta, pgid: metaPgid},
		pageContent: encodeMeta(meta),
	}

	wal.collectRecord(&record)
}

func (wal *WAL) collectRecord(record *Record) {
	wal.collectedRecords[record.header.pgid] = *record
}

func (wal *WAL) readRecords() ([]Record, error) {
	if !wal.hasRecords() {
		return nil, nil
	}

	var records []Record
	for {
		record, err := decodeRecord(wal.file, wal.db.meta.pageSize)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			// if we ever encouner an ErrChecksum, it is returned as an err
			return nil, err
		}
		records = append(records, *record)
	}

	return records, nil
}

func (wal *WAL) hasRecords() bool {
	fi, err := wal.file.Stat()
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

func (wal *WAL) sync() error {
	if !wal.hasUnsyncedWrites {
		return nil
	}

	var err error
	if wal.syncFile != nil {
		err = wal.syncFile()
	} else {
		// Production leaves syncFile nil and syncs the WAL file directly.
		err = wal.file.Sync()
	}
	if err != nil {
		return err
	}
	wal.hasUnsyncedWrites = false
	return nil
}

// Truncate the WAL file.
// Important: only truncate the file after making sure that it's content has been ingested to the database
func (wal *WAL) Truncate() error {
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := wal.file.Truncate(0); err != nil {
		_, seekErr := wal.file.Seek(0, io.SeekEnd)
		return errors.Join(err, seekErr)
	}
	return nil
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

func encodeRecord(record *Record, pageSize int64) ([]byte, error) {
	size, err := encodedRecordSize(record, pageSize)
	if err != nil {
		return nil, err
	}

	return appendEncodedRecord(make([]byte, 0, size), record), nil
}

func encodedRecordSize(record *Record, pageSize int64) (int, error) {
	if int64(len(record.pageContent)) > pageSize {
		return 0, fmt.Errorf("record page content exceeds page size (%d > %d)", len(record.pageContent), pageSize)
	}
	return recordHeaderSize + len(record.pageContent) + recordChecksumSize, nil
}

func appendEncodedRecord(data []byte, record *Record) []byte {
	headerStart := len(data)
	data = append(data, record.header.recordType)
	data = binary.LittleEndian.AppendUint64(data, uint64(record.header.pgid))
	data = binary.LittleEndian.AppendUint64(data, uint64(record.header.txid))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(record.pageContent)))
	headerEnd := len(data)
	data = append(data, record.pageContent...)
	checksum := computeRecordChecksum(data[headerStart:headerEnd], record.pageContent)
	data = binary.LittleEndian.AppendUint64(data, checksum)

	return data
}

func decodeRecord(r io.Reader, pageSize int64) (*Record, error) {
	headerData := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, headerData); err != nil {
		return nil, err // io.EOF at a clean boundary; io.ErrUnexpectedEOF on a torn tail
	}

	data := headerData
	record := &Record{}
	record.header.recordType = data[0]
	data = data[recordTypeSize:]
	record.header.pgid = Pgid(binary.LittleEndian.Uint64(data[:pgidEncodedSize]))
	data = data[pgidEncodedSize:]
	record.header.txid = Txid(binary.LittleEndian.Uint64(data[:recordTransactionIDSize]))
	data = data[recordTransactionIDSize:]
	contentSize := binary.LittleEndian.Uint32(data[:recordContentLengthSize])
	if int64(contentSize) > pageSize {
		// A torn tail can leave a bogus length prefix. Treat it as an integrity
		// failure (ErrChecksum) so readRecords stops at this record instead of
		// failing the whole open. A well-formed record can never exceed a page.
		return nil, fmt.Errorf("record content_size %d exceeds page size %d: %w", contentSize, pageSize, ErrChecksum)
	}

	record.pageContent = make([]byte, contentSize)
	if _, err := io.ReadFull(r, record.pageContent); err != nil {
		return nil, err
	}

	var checksumData [recordChecksumSize]byte
	if _, err := io.ReadFull(r, checksumData[:]); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint64(checksumData[:])
	if computeRecordChecksum(headerData, record.pageContent) != checksum {
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
	if err := writeFull(wal.db.file, page); err != nil {
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

	transaction, err := wal.encodeCollectedRecords()
	if err != nil {
		return err
	}

	needsCheckpoint, err := wal.appendTransaction(transaction)
	if err != nil {
		return err
	}

	wal.moveCollectedRecordsToOverlay()

	if needsCheckpoint {
		if err := wal.checkpoint(); err != nil {
			// The WAL is already durable. Keep the WAL and overlay so the
			// committed transaction stays readable and recoverable.
			return nil
		}
	}
	return nil
}

func (wal *WAL) encodeCollectedRecords() ([]byte, error) {
	transactionSize := recordHeaderSize + recordChecksumSize // The commit marker has no page content.
	for _, record := range wal.collectedRecords {
		size, err := encodedRecordSize(&record, wal.db.meta.pageSize)
		if err != nil {
			return nil, err
		}
		transactionSize += size
	}

	txid := wal.nextTxid
	wal.nextTxid++

	transaction := make([]byte, 0, transactionSize)

	for pgid, record := range wal.collectedRecords {
		record.header.txid = txid
		transaction = appendEncodedRecord(transaction, &record)

		wal.collectedRecords[pgid] = record
	}

	commitMarker := &Record{
		header:      RecordHeader{recordType: recordTypeCommit, pgid: 0, txid: txid},
		pageContent: nil,
	}
	transaction = appendEncodedRecord(transaction, commitMarker)

	return transaction, nil
}

func (wal *WAL) appendTransaction(transaction []byte) (bool, error) {
	if err := writeFull(wal.file, transaction); err != nil {
		return false, fmt.Errorf("persist multiple records: %w", err)
	}
	wal.hasUnsyncedWrites = true
	wal.bytesSinceCheckpoint += uint32(len(transaction))

	needsCheckpoint := wal.reachedCheckpointThreshold()
	if wal.db.options.synchronous == SYNCHRONOUS_FULL || needsCheckpoint {
		if err := wal.sync(); err != nil {
			return false, err
		}
	}

	return needsCheckpoint, nil
}

func (wal *WAL) moveCollectedRecordsToOverlay() {
	for key := range wal.collectedRecords {
		wal.overlay[key] = wal.collectedRecords[key]
	}
	clear(wal.collectedRecords)
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

	node, err := decodeNode(record.pageContent)

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
	if err := wal.sync(); err != nil {
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
		// The main file is already durable, so the transaction is committed.
		// Keep the WAL and overlay intact so cleanup can be retried safely.
		return nil
	}
	clear(wal.overlay)
	wal.bytesSinceCheckpoint = 0
	return nil
}
