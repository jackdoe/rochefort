package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
)

type StoreItem struct {
	path       string
	descriptor *os.File
	offset     int64
	mutex      sync.RWMutex
}
type Storage struct {
	files []*StoreItem
}

func NewStorage(root string, n int) *Storage {
	storage := &Storage{
		files: make([]*StoreItem, n),
	}
	for i := 0; i < n; i++ {
		filePath := path.Join(root, fmt.Sprintf("append.%d.raw", i))
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		offset, err := f.Seek(0, 2)

		if err != nil {
			panic(err)
		}
		log.Printf("openning: %s with offset: %d", filePath, offset)
		si := &StoreItem{
			offset:     offset,
			path:       filePath,
			descriptor: f,
		}
		storage.files[i] = si
	}
	return storage
}

func (this *Storage) read(sid string, offset int64) ([]byte, error) {
	id := hash(sid)
	file := this.files[id%uint32(len(this.files))]

	file.mutex.RLock()
	defer file.mutex.RUnlock()

	dataLenBytes := make([]byte, 4)
	_, err := file.descriptor.ReadAt(dataLenBytes, offset)
	if err != nil {
		return nil, err
	}

	var dataLen uint32
	buf := bytes.NewReader(dataLenBytes)
	err = binary.Read(buf, binary.LittleEndian, &dataLen)
	if err != nil {
		return nil, err
	}

	output := make([]byte, dataLen)
	_, err = file.descriptor.ReadAt(output, offset+4)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (this *Storage) append(sid string, data io.Reader) (int64, string) {
	id := hash(sid)
	file := this.files[id%uint32(len(this.files))]

	file.mutex.Lock()
	defer file.mutex.Unlock()

	currentOffset := file.offset
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return -1, ""
	}

	dataLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataLen, uint32(len(b)))

	written, err := file.descriptor.Write(dataLen)
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	written, err = file.descriptor.Write(b)
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	return currentOffset, file.path
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func main() {
	var pnBuckets = flag.Int("buckets", 128, "number of files to open")
	var pbind = flag.String("bind", ":8000", "address to bind to")
	var proot = flag.String("root", "/tmp", "root directory")
	flag.Parse()

	storage := NewStorage(*proot, *pnBuckets)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("\nReceived an interrupt, stopping services...\n")

		for _, file := range storage.files {
			file.mutex.Lock() // dont unlock it
			file.descriptor.Close()
			log.Printf("closing: %s", file.path)
		}
		os.Exit(0)

	}()

	http.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		offset, file := storage.append(r.URL.Query().Get("id"), r.Body)
		w.Write([]byte(fmt.Sprintf("{\"offset\":%d,\"file\":\"%s\"}", offset, file)))
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			data, err := storage.read(r.URL.Query().Get("id"), offset)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			} else {
				w.Write(data)
			}
		}
	})

	err := http.ListenAndServe(*pbind, nil)
	if err != nil {
		log.Fatal(err)
	}
}
