package kvlite

// TODO(component:page): fixed-size page abstraction — page header, page ids, page
// types (meta, freelist, branch, leaf), and reading/writing pages to the file/mmap.

type Pgid uint64
type Txid uint64
