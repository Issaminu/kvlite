package kvlite

import "testing"

func TestAllocationChangesCloneIsIndependent(t *testing.T) {
	bitmap := newAllocationBitmap(allocationHeaderSize + 1)
	changes := newAllocationChanges(bitmap)
	changes.markAllocated(4)

	clone := changes.clone()
	clone.markAllocated(5)

	if bitmap.allocated(4) {
		t.Fatal("private allocation changed the committed bitmap")
	}
	if changes.allocated(5) {
		t.Fatal("cloned allocation changed its parent")
	}
	if !clone.allocated(5) {
		t.Fatal("clone did not record its allocation")
	}
}
