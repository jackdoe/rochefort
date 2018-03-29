package main

import (
	"os"
	"path"
	"testing"
)

func TestCompact(t *testing.T) {
	path := path.Join(os.TempDir(), "rochefort_compact_test")
	os.RemoveAll(path)

	storage := NewStorage(path)

	data := []byte{1, 2, 3, 4}
	compactedOffset := uint32(0)
	uncompactedOffset := uint32(0)
	for i := 0; i < 100; i++ {
		allocSize := uint32(1024 + i)

		storage.append(allocSize, data)

		compactedOffset += uint32(len(data)) + headerLen
		uncompactedOffset += headerLen + allocSize
	}

	if uncompactedOffset != uint32(storage.offset) {
		t.Log("uncompactedOffset != stored.offset")
		t.FailNow()
	}

	if compactedOffset == uncompactedOffset {
		t.Log("compactedOffset == uncompactedOffset")
		t.FailNow()
	}

	for i := 0; i < 10; i++ {
		storage.compact()
		if compactedOffset != uint32(storage.offset) {
			t.Log("compactedOffset != stored.offset")
			t.FailNow()
		}
	}
	fi, err := storage.descriptor.Stat()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	if uint32(fi.Size()) != compactedOffset {
		t.Log("size !+ compactedOffset")
		t.FailNow()

	}
	os.RemoveAll(path)
}
