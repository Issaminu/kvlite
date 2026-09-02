//go:build windows

package kvlite

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// systemMapMainFile maps the first length bytes of file for read-only access.
//
// The file must already hold length bytes.
// A read-only mapping cannot extend a file.
// Windows rejects a mapping of an empty file.
// [DB.mapMainFile] maps the current file length, so it meets these conditions.
//
// A mapped view keeps its own reference to the mapping object.
// This function closes the mapping handle before it returns.
// The view then holds the last reference.
// [systemUnmapMainFile] releases that reference.
func systemMapMainFile(file *os.File, length int) ([]byte, error) {
	if length <= 0 {
		return nil, os.ErrInvalid
	}

	size := uint64(length)
	// The Windows API accepts the 64-bit mapping size as two 32-bit values.
	mappingSizeHigh := uint32(size >> 32)
	mappingSizeLow := uint32(size)
	mapping, err := windows.CreateFileMapping(
		windows.Handle(file.Fd()),
		nil,
		windows.PAGE_READONLY,
		mappingSizeHigh,
		mappingSizeLow,
		nil,
	)
	if err != nil {
		return nil, err
	}

	address, mapErr := windows.MapViewOfFile(mapping, windows.FILE_MAP_READ, 0, 0, uintptr(length))
	// Close the handle for both outcomes. A successful view stays valid without it.
	closeErr := windows.CloseHandle(mapping)
	if mapErr != nil {
		return nil, mapErr
	}
	if closeErr != nil {
		// The view is usable, but the leaked handle would keep the mapping object alive after the unmap.
		// Release the view and report the failure.
		_ = windows.UnmapViewOfFile(address)
		return nil, closeErr
	}

	// MapViewOfFile returns the base address of the view as a uintptr.
	// The address belongs to the operating system, not to the Go heap.
	// The garbage collector never moves or frees it.
	// It stays valid until systemUnmapMainFile runs.
	// "go vet" can report this conversion because it cannot see that guarantee.
	return unsafe.Slice((*byte)(unsafe.Pointer(address)), length), nil
}

// systemUnmapMainFile releases one mapped view.
// The caller must pass the complete slice that [systemMapMainFile] returned.
func systemUnmapMainFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}

// Windows does not guarantee that file writes update an existing mapped view.
func systemMainFileMappingIsCurrent(int64, int64) bool {
	return false
}
