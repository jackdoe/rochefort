package main

import (
	"bytes"
	"fmt"
	mr "math/rand"
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

type Thing struct {
	data      []byte
	allocSize uint32
}

func NewThing() *Thing {
	size := mr.Int31n(10240) + 1024
	allocSize := size * 2
	return &Thing{
		allocSize: uint32(allocSize),
	}
}

func (t *Thing) mutate() {
	t.data = make([]byte, (mr.Int31n(int32(t.allocSize))))
	mr.Read(t.data)
}

type Corruptor struct {
	storage   *StoreItem
	things    map[uint64]*Thing
	progressA string
}

func NewCorruptor(n int, storage *StoreItem) *Corruptor {
	return &Corruptor{
		storage:   storage,
		things:    map[uint64]*Thing{},
		progressA: fmt.Sprintf("[c: %d]", n),
	}
}

func (c *Corruptor) Smash(done chan bool) {
	bytesSoFar := 0
	modifiedSoFar := 0
	for i := 0; i < 100; i++ {
		t := NewThing()
		t.mutate()
		off, err := c.storage.append(t.allocSize, t.data)
		bytesSoFar += len(t.data)
		if err != nil {
			panic(err)
		}
		c.things[off] = t

		for off, t := range c.things {
			data, err := c.storage.read(off)
			if err != nil {
				panic(err)
			}

			if !bytes.Equal(data, t.data) {
				panic("not equals")
			}
			t.mutate()
			modifiedSoFar += len(t.data)
			err = c.storage.modify(off, 0, t.data, true)
			if err != nil {
				panic(err)
			}
		}
		if i%10 == 0 {
			fmt.Printf("%s: %d, bytesSoFar: %.2fMB, modifiedSoFar: %.2fMB\n", c.progressA, i, float32(bytesSoFar)/1024/1024, float32(modifiedSoFar)/1024/1024)
		}
	}

	done <- true
}

func (c *Corruptor) Test(relocationMap map[uint64]uint64) {
	i := 0
	for off, t := range c.things {

		data, err := c.storage.read(relocationMap[off])
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(data, t.data) {
			panic("not equals")
		}
		i++
		if i%100 == 0 {
			fmt.Printf("[sample]%s: offset %d moved to %d after compaction\n", c.progressA, off, relocationMap[off])
		}
	}
}

func TestForever(t *testing.T) {
	path := path.Join(os.TempDir(), "smash")
	os.RemoveAll(path)

	for k := 0; k < 10; k++ {
		done := make(chan bool, 1)
		n := 100
		storage := NewStorage(path)
		corruptors := []*Corruptor{}
		for i := 0; i < n; i++ {
			c := NewCorruptor(i, storage)
			corruptors = append(corruptors, c)
			go c.Smash(done)
		}
		i := 0
		for {
			<-done
			i++
			if i == n {
				break
			}
		}

		relocationMap, err := storage.compact()
		if err != nil {
			panic(err)
		}

		for _, c := range corruptors {
			c.Test(relocationMap)
		}
	}
}
