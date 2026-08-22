package btree

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/Issaminu/kvlite/internal/page"
)

type cursorErrorStore struct {
	memoryTreeStore
	readErr error
}

func (store *cursorErrorStore) ReadNode(page.ID) (*Node, error) {
	return nil, store.readErr
}

func requireCursorEntry(t *testing.T, entry *Entry, found bool, err error, wantKey, wantValue []byte) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("cursor entry %q: not found", wantKey)
	}
	if !bytes.Equal(entry.Key(), wantKey) || !bytes.Equal(entry.Value(), wantValue) {
		t.Fatalf("cursor entry: got key=%q value=%q, want key=%q value=%q", entry.Key(), entry.Value(), wantKey, wantValue)
	}
}

func newCursorTestTree(t *testing.T, entryCount int) (*Tree, *Node) {
	t.Helper()
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
	root := NewLeafNode(1)
	store.StageNode(root)
	for index := 0; index < entryCount; index++ {
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
	store := &memoryTreeStore{pageSize: 128, nextID: 2, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
	root := NewLeafNode(1)

	cursor := tree.Cursor(root)
	if _, found, err := cursor.First(); err != nil || found {
		t.Fatalf("first empty entry: found=%t err=%v", found, err)
	}
	if _, found, err := cursor.Last(); err != nil || found {
		t.Fatalf("last empty entry: found=%t err=%v", found, err)
	}
}

func TestCursor_SingleLeaf(t *testing.T) {
	store := &memoryTreeStore{pageSize: 4096, nextID: 2, nodes: make(map[page.ID]*Node)}
	tree := NewTree(store)
	root := NewLeafNode(1)
	for _, key := range []string{"a", "c", "e"} {
		if err := root.InsertEntry(NewEntry(0, []byte(key), []byte("value-"+key))); err != nil {
			t.Fatal(err)
		}
	}

	cursor := tree.Cursor(root)
	entry, found, err := cursor.First()
	requireCursorEntry(t, entry, found, err, []byte("a"), []byte("value-a"))
	entry, found, err = cursor.Next()
	requireCursorEntry(t, entry, found, err, []byte("c"), []byte("value-c"))
	entry, found, err = cursor.Last()
	requireCursorEntry(t, entry, found, err, []byte("e"), []byte("value-e"))
	entry, found, err = cursor.Prev()
	requireCursorEntry(t, entry, found, err, []byte("c"), []byte("value-c"))
}

func TestCursor_MultiLevel(t *testing.T) {
	const entryCount = 100
	tree, root := newCursorTestTree(t, entryCount)
	if root.IsLeaf() {
		t.Fatal("test tree root is still a leaf")
	}

	cursor := tree.Cursor(root)
	entry, found, err := cursor.First()
	for index := 0; index < entryCount; index++ {
		wantKey := fmt.Appendf(nil, "key-%03d", index)
		wantValue := fmt.Appendf(nil, "value-%03d", index)
		requireCursorEntry(t, entry, found, err, wantKey, wantValue)
		entry, found, err = cursor.Next()
	}
	if err != nil || found {
		t.Fatalf("entry after final key: found=%t err=%v", found, err)
	}

	entry, found, err = cursor.Last()
	for index := entryCount - 1; index >= 0; index-- {
		wantKey := fmt.Appendf(nil, "key-%03d", index)
		wantValue := fmt.Appendf(nil, "value-%03d", index)
		requireCursorEntry(t, entry, found, err, wantKey, wantValue)
		entry, found, err = cursor.Prev()
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
			entry, found, err := tree.Cursor(root).Seek(testCase.target)
			if !testCase.wantFound {
				if err != nil || found {
					t.Fatalf("seek %q: found=%t err=%v", testCase.target, found, err)
				}
				return
			}
			requireCursorEntry(t, entry, found, err, testCase.wantKey, testCase.wantValue)
		})
	}
}

func TestCursor_MissingChildReturnsKeyNotFound(t *testing.T) {
	left := NewLeafNode(2)
	if err := left.InsertEntry(NewEntry(0, []byte("a"), []byte("value"))); err != nil {
		t.Fatal(err)
	}
	root := NewRootNode(1, left, NewLeafNode(3), []byte("m"))
	store := &memoryTreeStore{pageSize: 4096, nodes: map[page.ID]*Node{2: left}}

	_, found, err := NewTree(store).Cursor(root).Seek([]byte("z"))
	if found || !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing child: found=%t err=%v", found, err)
	}
}

func TestCursor_ReturnsPageReadError(t *testing.T) {
	errRead := errors.New("read node")
	left := NewLeafNode(2)
	root := NewRootNode(1, left, NewLeafNode(3), []byte("m"))
	store := &cursorErrorStore{memoryTreeStore: memoryTreeStore{pageSize: 4096}, readErr: errRead}

	_, found, err := NewTree(store).Cursor(root).First()
	if found || !errors.Is(err, errRead) {
		t.Fatalf("page read error: found=%t err=%v", found, err)
	}
}
