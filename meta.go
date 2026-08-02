package kvlite

// TODO(component:meta): meta page format (magic, version, root, freelist pgid,
// txid, checksum) + the two-meta-page copy-on-write commit and validation on open.
type Meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     string // hmm
	freelist Pgid
	pgid     Pgid
	txid     Txid
	checksum uint64
}
