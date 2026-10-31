package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

type cursorErrorStore struct {
	memoryTreeStore
	readErr error
}

func (store *cursorErrorStore) ReadNode(page.ID) (Node, error) {
	return Node{}, store.readErr
}

func requireCursorEntry(t *testing.T, cursor *Cursor, found bool, err error, wantKey, wantValue []byte) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("cursor entry %q: not found", wantKey)
	}
	_, key, value := cursor.Entry()
	if !bytes.Equal(key, wantKey) || !bytes.Equal(value, wantValue) {
		t.Fatalf("cursor entry: got key=%q value=%q, want key=%q value=%q", key, value, wantKey, wantValue)
	}
}

func newCursorTestTree(t *testing.T, entryCount int) (*testTree, *MutableNode) {
	t.Helper()
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(1)
	store.StageNode(root)
	for index := range entryCount {
		key := fmt.Appendf(nil, "key-%03d", index)
		value := fmt.Appendf(nil, "value-%03d", index)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, value))
		if err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	return tree, root
}

func TestCursor_Empty(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(1)

	cursor := tree.Cursor(root)
	if found, err := cursor.First(); err != nil || found {
		t.Fatalf("first empty entry: found=%t err=%v", found, err)
	}
	if found, err := cursor.Last(); err != nil || found {
		t.Fatalf("last empty entry: found=%t err=%v", found, err)
	}
}

func TestCursor_SingleLeaf(t *testing.T) {
	store := &memoryTreeStore{pageSize: 4096, nextID: 2, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(1)
	for _, key := range []string{"a", "c", "e"} {
		if err := root.InsertEntry(NewEntry(0, []byte(key), []byte("value-"+key))); err != nil {
			t.Fatal(err)
		}
	}

	cursor := tree.Cursor(root)
	found, err := cursor.First()
	requireCursorEntry(t, cursor, found, err, []byte("a"), []byte("value-a"))
	found, err = cursor.Next()
	requireCursorEntry(t, cursor, found, err, []byte("c"), []byte("value-c"))
	found, err = cursor.Last()
	requireCursorEntry(t, cursor, found, err, []byte("e"), []byte("value-e"))
	found, err = cursor.Prev()
	requireCursorEntry(t, cursor, found, err, []byte("c"), []byte("value-c"))
}

func TestCursor_MultiLevel(t *testing.T) {
	const entryCount = 100
	tree, root := newCursorTestTree(t, entryCount)
	if root.IsLeaf() {
		t.Fatal("test tree root is still a leaf")
	}

	cursor := tree.Cursor(root)
	found, err := cursor.First()
	for index := range entryCount {
		wantKey := fmt.Appendf(nil, "key-%03d", index)
		wantValue := fmt.Appendf(nil, "value-%03d", index)
		requireCursorEntry(t, cursor, found, err, wantKey, wantValue)
		found, err = cursor.Next()
	}
	if err != nil || found {
		t.Fatalf("entry after final key: found=%t err=%v", found, err)
	}

	found, err = cursor.Last()
	for index := entryCount - 1; index >= 0; index-- {
		wantKey := fmt.Appendf(nil, "key-%03d", index)
		wantValue := fmt.Appendf(nil, "value-%03d", index)
		requireCursorEntry(t, cursor, found, err, wantKey, wantValue)
		found, err = cursor.Prev()
	}
	if err != nil || found {
		t.Fatalf("entry before first key: found=%t err=%v", found, err)
	}
}

func TestCursor_Seek(t *testing.T) {
	tree, root := newCursorTestTree(t, 100)
	testCases := []struct {
		name      string
		target    []byte
		wantKey   []byte
		wantValue []byte
		wantFound bool
	}{
		{name: "first exact", target: []byte("key-000"), wantKey: []byte("key-000"), wantValue: []byte("value-000"), wantFound: true},
		{name: "middle exact", target: []byte("key-049"), wantKey: []byte("key-049"), wantValue: []byte("value-049"), wantFound: true},
		{name: "lower bound", target: []byte("key-049x"), wantKey: []byte("key-050"), wantValue: []byte("value-050"), wantFound: true},
		{name: "empty target", target: nil, wantKey: []byte("key-000"), wantValue: []byte("value-000"), wantFound: true},
		{name: "after last", target: []byte("key-999"), wantFound: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cursor := tree.Cursor(root)
			found, err := cursor.Seek(testCase.target)
			if !testCase.wantFound {
				if err != nil || found {
					t.Fatalf("seek %q: found=%t err=%v", testCase.target, found, err)
				}
				return
			}
			requireCursorEntry(t, cursor, found, err, testCase.wantKey, testCase.wantValue)
		})
	}
}

func TestCursor_MissingChildReturnsKeyNotFound(t *testing.T) {
	left := NewLeafNode(2)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	root := NewRootNode(1, left.PageID(), NewLeafNode(3).PageID(), []byte("m"))
	store := &memoryTreeStore{pageSize: 4096, nodes: map[page.ID]*MutableNode{2: left}}

	found, err := newTestTree(store).Cursor(root).Seek([]byte("z"))
	if found || !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing child: found=%t err=%v", found, err)
	}
}

func TestCursor_ReturnsPageReadError(t *testing.T) {
	errRead := errors.New("read node")
	left := NewLeafNode(2)
	root := NewRootNode(1, left.PageID(), NewLeafNode(3).PageID(), []byte("m"))
	store := &cursorErrorStore{memoryTreeStore: memoryTreeStore{pageSize: 4096}, readErr: errRead}

	found, err := newTestTree(store).Cursor(root).First()
	if found || !errors.Is(err, errRead) {
		t.Fatalf("page read error: found=%t err=%v", found, err)
	}
}

func TestCursorSeekForwardReusesLeafAndRetainsPath(t *testing.T) {
	tree, root := newCursorTestTree(t, 100)
	cursor := tree.Cursor(root)
	found, err := cursor.SeekForward([]byte("key-050"))
	requireCursorEntry(t, cursor, found, err, []byte("key-050"), []byte("value-050"))
	leaf := cursor.leaf.node
	original := cursor.reader
	failure := errors.New("unexpected child read")
	cursor.reader = &cursorErrorStore{readErr: failure}
	for _, index := range []int{0, len(leaf.entries) - 1, 0} {
		found, err := cursor.SeekForward(leaf.entries[index].key)
		requireCursorEntry(t, cursor, found, err, leaf.entries[index].key, leaf.entries[index].value)
	}
	if _, err := cursor.SeekForward([]byte("zzzz")); !errors.Is(err, failure) {
		t.Fatalf("cross-leaf seek did not read its path: %v", err)
	}
	if found, err := cursor.Next(); err != nil || found {
		t.Fatalf("failed seek retained a stale position: %t %v", found, err)
	}
	cursor.reader = original
	found, err = cursor.SeekForward([]byte("key-050"))
	requireCursorEntry(t, cursor, found, err, []byte("key-050"), []byte("value-050"))
	found, err = cursor.Next()
	requireCursorEntry(t, cursor, found, err, []byte("key-051"), []byte("value-051"))
	found, err = cursor.Prev()
	requireCursorEntry(t, cursor, found, err, []byte("key-050"), []byte("value-050"))
	found, err = cursor.Prev()
	requireCursorEntry(t, cursor, found, err, []byte("key-049"), []byte("value-049"))
}

func TestCursor_FindPreservesIterationPosition(t *testing.T) {
	for _, count := range []int{3, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			tree, root := newCursorTestTree(t, count)
			cursor := tree.Cursor(root)
			found, err := cursor.Seek([]byte("key-001"))
			requireCursorEntry(t, cursor, found, err, []byte("key-001"), []byte("value-001"))
			for _, includeKey := range []bool{false, true} {
				got, found, err := cursor.Find([]byte("key-002"), includeKey)
				if err != nil || !found || string(got.Value()) != "value-002" {
					t.Fatalf("exact hit: found=%t value=%q error=%v", found, got.Value(), err)
				}
				if includeKey && string(got.Key()) != "key-002" {
					t.Fatalf("exact key: got %q", got.Key())
				}
				if _, found, err := cursor.Find([]byte("key-001x"), includeKey); err != nil || found {
					t.Fatalf("exact miss: found=%t error=%v", found, err)
				}
			}
			for _, test := range []struct {
				key  []byte
				want error
			}{
				{nil, ErrKeyRequired},
				{make([]byte, MaxKeySize+1), ErrKeyTooLarge},
			} {
				if _, found, err := cursor.Find(test.key, true); found || !errors.Is(err, test.want) {
					t.Fatalf("invalid key: found=%t error=%v, want %v", found, err, test.want)
				}
			}
			found, err = cursor.Next()
			requireCursorEntry(t, cursor, found, err, []byte("key-002"), []byte("value-002"))
		})
	}
}

func TestCursor_FindReturnsReaderError(t *testing.T) {
	tree, root := newCursorTestTree(t, 100)
	want := errors.New("exact lookup read failed")
	store := &cursorErrorStore{memoryTreeStore: *tree.store.(*memoryTreeStore), readErr: want}
	if _, found, err := newTestTree(store).Cursor(root).Find([]byte("key-002"), true); found || !errors.Is(err, want) {
		t.Fatalf("reader failure: found=%t error=%v, want %v", found, err, want)
	}
}

type cursorViewReader struct{ views map[page.ID]Node }

func (reader *cursorViewReader) ReadNode(id page.ID) (Node, error) {
	view, ok := reader.views[id]
	if !ok {
		return Node{}, ErrKeyNotFound
	}
	return view, nil
}

func TestCursorRejectsMalformedPackedEntries(t *testing.T) {
	leaf := NewLeafNode(2)
	for _, key := range []string{"a", "c"} {
		if err := leaf.InsertEntry(NewEntry(0, []byte(key), []byte("value"))); err != nil {
			t.Fatal(err)
		}
	}
	for _, change := range []string{"gap", "duplicate", "order", "empty"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprint(change, reverse), func(t *testing.T) {
				data := make([]byte, 128)
				if err := EncodeNodeInto(data, leaf, 128, false); err != nil {
					t.Fatal(err)
				}
				first, second := NodeHeaderSize, NodeHeaderSize+leafEntryDescriptorSize
				switch change {
				case "empty":
					binary.LittleEndian.PutUint32(data[10:14], 0)
					binary.LittleEndian.PutUint32(data[18:22], 1)
				case "gap":
					binary.LittleEndian.PutUint32(data[first+12:first+16], 4)
				case "duplicate":
					copy(data[second:second+leafEntryDescriptorSize], data[first:first+leafEntryDescriptorSize])
				case "order":
					data[binary.LittleEndian.Uint32(data[first+4:first+8])] = 'z'
					data[binary.LittleEndian.Uint32(data[second+4:second+8])] = 'a'
				}
				view, err := ReadNodeImage(data, 2, 128, false, false)
				if change == "empty" {
					if !errors.Is(err, ErrInvalid) {
						t.Fatalf("malformed empty leaf: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				cursor := NewCursor(&cursorViewReader{views: map[page.ID]Node{2: view}}, 2)
				var found bool
				if reverse {
					found, err = cursor.Last()
				} else {
					found, err = cursor.First()
				}
				for count := 0; err == nil && found && count < 3; count++ {
					if reverse {
						found, err = cursor.Prev()
					} else {
						found, err = cursor.Next()
					}
				}
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("malformed %s: %v", change, err)
				}
				if found, err := cursor.Next(); err != nil || found {
					t.Fatalf("failed read retained position: %t %v", found, err)
				}
			})
		}
	}
}

func TestCursorRejectsMalformedBranchSeparators(t *testing.T) {
	left, right := NewLeafNode(3), NewLeafNode(4)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), []byte("left"))); err != nil {
		t.Fatal(err)
	}
	if err := right.InsertEntry(NewEntry(0, []byte("z"), []byte("right"))); err != nil {
		t.Fatal(err)
	}
	root := NewRootNode(2, left.PageID(), right.PageID(), []byte("m"))
	for _, reverse := range []bool{false, true} {
		data := make([]byte, 128)
		if err := EncodeNodeInto(data, root, 128, false); err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(data[NodeHeaderSize:NodeHeaderSize+4], uint32(len(data)))
		view, err := ReadNodeImage(data, 2, 128, false, false)
		if err != nil {
			t.Fatal(err)
		}
		reader := &cursorViewReader{views: map[page.ID]Node{2: view, 3: left.ReadOnly(), 4: right.ReadOnly()}}
		cursor := NewCursor(reader, 2)
		if reverse {
			_, err = cursor.Last()
		} else {
			_, err = cursor.First()
		}
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("reverse=%t: malformed branch: %v", reverse, err)
		}
	}
}

func TestCursorEncodedAndMixedViews(t *testing.T) {
	store := &memoryTreeStore{pageSize: 128, nextID: 3, nodes: make(map[page.ID]*MutableNode)}
	tree := newTestTree(store)
	root := NewLeafNode(2)
	store.StageNode(root)
	for index := range 100 {
		key := fmt.Appendf(nil, "key-%03d", index)
		var err error
		root, err = tree.PutEntry(root, NewEntry(0, key, key))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"main", "checksums", "wal", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			reader := &cursorViewReader{views: make(map[page.ID]Node)}
			for id, node := range store.nodes {
				if mode == "mixed" && id%2 == 0 {
					reader.views[id] = node.ReadOnly()
					continue
				}
				var data []byte
				if mode == "wal" {
					data = EncodeWALNode(node)
				} else {
					data = make([]byte, NodePageCount(node, 128)*128)
					if err := EncodeNodeInto(data, node, 128, mode == "checksums"); err != nil {
						t.Fatal(err)
					}
				}
				view, err := ReadNodeImage(data, id, 128, mode == "wal", mode == "checksums")
				if err != nil {
					t.Fatal(err)
				}
				reader.views[id] = view
			}
			cursor := NewCursor(reader, root.PageID())
			found, err := cursor.First()
			for index := range 100 {
				key := fmt.Appendf(nil, "key-%03d", index)
				requireCursorEntry(t, &cursor, found, err, key, key)
				found, err = cursor.Next()
			}
			if err != nil || found {
				t.Fatalf("forward exhaustion: %t %v", found, err)
			}
			found, err = cursor.Last()
			for index := 99; index >= 0; index-- {
				key := fmt.Appendf(nil, "key-%03d", index)
				requireCursorEntry(t, &cursor, found, err, key, key)
				found, err = cursor.Prev()
			}
			if err != nil || found {
				t.Fatalf("reverse exhaustion: %t %v", found, err)
			}
			found, err = cursor.Seek([]byte("key-042x"))
			requireCursorEntry(t, &cursor, found, err, []byte("key-043"), []byte("key-043"))
			found, err = cursor.Next()
			requireCursorEntry(t, &cursor, found, err, []byte("key-044"), []byte("key-044"))
			found, err = cursor.Prev()
			requireCursorEntry(t, &cursor, found, err, []byte("key-043"), []byte("key-043"))
			found, err = cursor.Next()
			requireCursorEntry(t, &cursor, found, err, []byte("key-044"), []byte("key-044"))
			_, found, err = cursor.Find([]byte("key-042x"), true)
			if err != nil || found {
				t.Fatalf("exact missing: %t %v", found, err)
			}
		})
	}
}

func TestCursorEntryClearsAfterFailedMovement(t *testing.T) {
	for _, operation := range []string{"first", "last", "seek", "next", "prev"} {
		t.Run(operation, func(t *testing.T) {
			tree, root := newCursorTestTree(t, 100)
			cursor := tree.Cursor(root)
			found, err := cursor.Last()
			if operation == "next" {
				found, err = cursor.First()
				if err == nil && found {
					found, err = cursor.Seek(cursor.leaf.node.entries[len(cursor.leaf.node.entries)-1].key)
				}
			} else if operation == "prev" && err == nil && found {
				found, err = cursor.Seek(cursor.leaf.node.entries[0].key)
			}
			if err != nil || !found {
				t.Fatalf("initial position: %t %v", found, err)
			}
			want := errors.New("page read failed")
			cursor.reader = &cursorErrorStore{readErr: want}
			switch operation {
			case "first":
				found, err = cursor.First()
			case "last":
				found, err = cursor.Last()
			case "seek":
				found, err = cursor.Seek([]byte("key-050"))
			case "next":
				found, err = cursor.Next()
			case "prev":
				found, err = cursor.Prev()
			}
			if found || !errors.Is(err, want) {
				t.Fatalf("failed move: %t %v", found, err)
			}
			flags, key, value := cursor.Entry()
			if flags != 0 || key != nil || value != nil {
				t.Fatalf("failed move retained entry: %d %q %q", flags, key, value)
			}
			if found, err := cursor.Next(); found || err != nil {
				t.Fatalf("move after failure: %t %v", found, err)
			}
		})
	}
}
