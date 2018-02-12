package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/dgryski/go-metro"
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
	"time"
)

type StoreItem struct {
	path       string
	descriptor *os.File
	offset     int64
	sync.Mutex
}
type Storage struct {
	files []*StoreItem
}

func NewStorage(root string, n int) *Storage {
	storage := &Storage{
		files: make([]*StoreItem, n),
	}

	os.MkdirAll(root, 0700)

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

func (this *Storage) scan(cb func(uint32, uint64, []byte)) {
	for fileIdx, f := range this.files {
	SCAN:
		for offset := int64(0); offset < f.offset; {
			// this is lockless, which means we could read a header,
			// but the data might be incomplete

			dataLen, err := readHeader(f.descriptor, uint64(offset))
			if err != nil {
				break SCAN
			}
			output := make([]byte, dataLen)
			_, err = f.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
			if err != nil {
				break SCAN
			}

			cb(dataLen, encodedOffset(fileIdx, offset), output)
			offset += int64(dataLen + headerLen)
		}
	}
}

const headerLen = 4 + 8 + 4

func readHeader(file *os.File, offset uint64) (uint32, error) {
	headerBytes := make([]byte, headerLen)
	_, err := file.ReadAt(headerBytes, int64(offset))
	if err != nil {
		return 0, err
	}
	dataLen := binary.LittleEndian.Uint32(headerBytes[0:])

	// no need for it
	// timeNano := binary.LittleEndian.Uint64(headerBytes[4:])

	checksum := binary.LittleEndian.Uint32(headerBytes[12:])

	if checksum != crc(headerBytes[0:12]) {
		return 0, errors.New("wrong checksum")
	}
	return dataLen, nil
}

func (this *Storage) read(offset uint64) (uint32, []byte, error) {
	// lockless read
	fileIndex := offset >> 50
	offset = offset & 0x0000FFFFFFFFFFFF
	if fileIndex > uint64(len(this.files)-1) {
		return 0, nil, errors.New("wrong offset, index > open files")
	}
	file := this.files[fileIndex]

	dataLen, err := readHeader(file.descriptor, offset)
	if err != nil {
		return 0, nil, err
	}

	output := make([]byte, dataLen)
	_, err = file.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
	if err != nil {
		return 0, nil, err
	}
	return dataLen, output, nil
}

func (this *Storage) append(sid string, data io.Reader) (uint64, string, error) {
	id := hash(sid)
	fileIndex := int(id % uint32(len(this.files)))
	file := this.files[fileIndex]

	file.Lock()
	defer file.Unlock()

	currentOffset := file.offset

	b, err := ioutil.ReadAll(data)
	if err != nil {
		return 0, "", err
	}
	header := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(header[0:], uint32(len(b)))
	binary.LittleEndian.PutUint64(header[4:], uint64(time.Now().UnixNano()))

	checksum := crc(header[0:12])
	binary.LittleEndian.PutUint32(header[12:], checksum)

	written, err := file.descriptor.Write(header)
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	written, err = file.descriptor.Write(b)
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	return encodedOffset(fileIndex, currentOffset), file.path, nil
}

func encodedOffset(fileIndex int, offset int64) uint64 {
	return (uint64(fileIndex) << uint64(50)) | uint64(offset)
}
func hash(s string) uint32 {
	return uint32(metro.Hash64Str(s, 0) >> uint64(32))
}

func crc(b []byte) uint32 {
	return uint32(metro.Hash64(b, 0) >> uint64(32))
}

type MultiStore struct {
	stores   map[string]*Storage
	nBuckets int
	root     string
	sync.RWMutex
}

func (this *MultiStore) find(storageIdentifier string) *Storage {
	if storageIdentifier == "" {
		storageIdentifier = "default"
	}

	this.RLock()
	storage, ok := this.stores[storageIdentifier]
	this.RUnlock()
	if !ok {
		this.Lock()
		defer this.Unlock()
		storage, ok = this.stores[storageIdentifier]

		if !ok {
			storage = NewStorage(path.Join(this.root, storageIdentifier), this.nBuckets)
			this.stores[storageIdentifier] = storage
		}
	}
	return storage
}

func (this *MultiStore) close(storageIdentifier string) {
	this.Lock()
	defer this.Unlock()
	if storageIdentifier == "" {
		storageIdentifier = "default"
	}
	storage, ok := this.stores[storageIdentifier]
	if ok {
		for _, file := range storage.files {
			file.descriptor.Close()
			log.Printf("closing: %s", file.path)
		}
	}
	delete(this.stores, storageIdentifier)
}

func (this *MultiStore) append(storageIdentifier, sid string, data io.Reader) (uint64, string, error) {
	return this.find(storageIdentifier).append(sid, data)
}

func (this *MultiStore) read(storageIdentifier string, offset uint64) (uint32, []byte, error) {
	return this.find(storageIdentifier).read(offset)
}

func (this *MultiStore) scan(storageIdentifier string, cb func(uint32, uint64, []byte)) {
	this.find(storageIdentifier).scan(cb)
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func Log(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := makeTimestamp()
		handler.ServeHTTP(w, r)
		log.Printf("%s %s %s took: %d", r.RemoteAddr, r.Method, r.URL, makeTimestamp()-t0)
	})
}

const storagePrefixKey = "storagePrefix"
const idKey = "id"
const offsetKey = "offset"

func main() {
	var pnBuckets = flag.Int("buckets", 128, "number of files to open")
	var pbind = flag.String("bind", ":8000", "address to bind to")
	var proot = flag.String("root", "/tmp", "root directory")
	flag.Parse()

	if *pnBuckets > 8191 {
		log.Fatalf("buckets can be at most 8191, we store them in 13 bits (returned offsets are bucket << 50 | offset)")
	}

	multiStore := &MultiStore{
		stores:   make(map[string]*Storage),
		nBuckets: *pnBuckets,
		root:     *proot,
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("\nReceived an interrupt, stopping services...\n")
		multiStore.Lock() // dont unlock it
		for _, storage := range multiStore.stores {
			for _, file := range storage.files {
				file.Lock() // dont unlock it
				file.descriptor.Close()
				log.Printf("closing: %s", file.path)
			}
		}
		os.Exit(0)

	}()

	http.HandleFunc("/close", func(w http.ResponseWriter, r *http.Request) {
		multiStore.close(r.URL.Query().Get(storagePrefixKey))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"success\":true}"))
	})

	http.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		offset, file, err := multiStore.append(r.URL.Query().Get(storagePrefixKey), r.URL.Query().Get(idKey), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf("{\"offset\":%d,\"file\":\"%s\"}", offset, file)))
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseUint(r.URL.Query().Get(offsetKey), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			_, data, err := multiStore.read(r.URL.Query().Get(storagePrefixKey), offset)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			} else {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write(data)
			}
		}
	})

	http.HandleFunc("/scan", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")

		header := make([]byte, 12)
		multiStore.scan(r.URL.Query().Get(storagePrefixKey), func(dataLen uint32, offset uint64, data []byte) {
			binary.LittleEndian.PutUint32(header[0:], uint32(len(data)))
			binary.LittleEndian.PutUint64(header[4:], offset)

			w.Write(header)
			w.Write(data)
		})
	})

	http.HandleFunc("/getMulti", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		dataLenRaw := make([]byte, 4)
		defer r.Body.Close()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			binary.LittleEndian.PutUint32(dataLenRaw, 0)
			w.Write([]byte(fmt.Sprintf("read: %s", err.Error())))
			return
		}
		storagePrefix := r.URL.Query().Get(storagePrefixKey)
		for i := 0; i < len(b); i += 8 {
			offset := binary.LittleEndian.Uint64(b[i:])
			_, data, err := multiStore.read(storagePrefix, offset)

			// XXX: we ignore the error on purpose
			// as the storage is not fsyncing, it could very well lose some updates
			// also the data is barely checksummed, so might very well be corrupted
			if err == nil {
				binary.LittleEndian.PutUint32(dataLenRaw, uint32(len(data)))
				w.Write(dataLenRaw)
				w.Write(data)
			}
		}
	})
	log.Printf("starting http server on %s", *pbind)
	err := http.ListenAndServe(*pbind, Log(http.DefaultServeMux))
	if err != nil {
		log.Fatal(err)
	}
}
