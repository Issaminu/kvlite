package btree

import (
	"github.com/Issaminu/kvlite/internal/page"
)

// ContinuationHeaderSize is the fixed 18-byte continuation header size.
const ContinuationHeaderSize = 18

const continuationChecksumOffset = 10

// ContinuationHeader identifies one continuation page. Integers are little-endian.
//
//	Bytes          Size   Field
//	[0:2]          2      Type: NodeTypeContinuation (0x0003)
//	[2:10]         8      PageID: this page's ID
//	[10:14]        4      CRC32C: covers all bytes except [10:14]
//	[14:18]        4      NextPageCount: N
//	[18:18+8*N]    N*8    next page IDs in body order
//	[18+8*N:P]     rest   body fragment, including final-page padding
//
// N=0 means no ID array. An array's last page can contain another array.
// Every page holds body bytes. There is no entry count or body-length field.
type ContinuationHeader struct {
	Type          NodeType
	PageID        page.ID
	Checksum      uint32
	NextPageCount uint32
}

// ContinuationNode contains a decoded continuation page, not a tree child.
// Its header and next page IDs own their values. Its body owns the encoded
// bytes passed to DecodeContinuationNode and includes final-page padding.
type ContinuationNode struct {
	header      *ContinuationHeader
	nextPageIDs []page.ID
	body        []byte
}

// NewContinuationNode takes ownership of ids and body. Do not change those
// slices after this call. Encoding adds any required zero padding.
func NewContinuationNode(id page.ID, ids []page.ID, body []byte) *ContinuationNode {
	return &ContinuationNode{header: &ContinuationHeader{Type: NodeTypeContinuation, PageID: id, NextPageCount: uint32(len(ids))}, nextPageIDs: ids, body: body}
}

// PageID returns this continuation page's ID.
func (n *ContinuationNode) PageID() page.ID { return n.header.PageID }

// Header returns a copy of the decoded header.
func (n *ContinuationNode) Header() ContinuationHeader { return *n.header }

// NextPageIDs returns this page's next group of storage pages. Do not change it.
func (n *ContinuationNode) NextPageIDs() []page.ID { return n.nextPageIDs }

// Body returns this page's body bytes. Do not change them.
func (n *ContinuationNode) Body() []byte { return n.body }
