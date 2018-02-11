package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
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
	"time"
)

type StoreItem struct {
	path       string
	descriptor *os.File
	offset     int64
	sync.RWMutex
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

func (this *Storage) scan(cb func(uint32, []byte)) {
	for _, f := range this.files {
	SCAN:
		for offset := int64(0); offset < f.offset; {
			dataLen, err := readHeader(f.descriptor, uint64(offset))
			if err != nil {
				break SCAN
			}
			output := make([]byte, dataLen)
			_, err = f.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
			if err != nil {
				if err != nil {
					break SCAN
				}

			}
			cb(dataLen, output)
			offset += int64(dataLen + headerLen)
		}
	}
}

const headerLen = 4 + 8 + 4

func readHeader(file *os.File, offset uint64) (uint32, error) {
	dataLenBytes := make([]byte, headerLen)
	_, err := file.ReadAt(dataLenBytes, int64(offset))
	if err != nil {
		return 0, err
	}

	var dataLen uint32
	var timeNano uint64
	var checksum uint32
	buf := bytes.NewReader(dataLenBytes)
	err = binary.Read(buf, binary.LittleEndian, &dataLen)
	if err != nil {
		return 0, err
	}

	err = binary.Read(buf, binary.LittleEndian, &timeNano)
	if err != nil {
		return 0, err
	}

	err = binary.Read(buf, binary.LittleEndian, &checksum)
	if err != nil {
		return 0, err
	}
	checksumBytes := make([]byte, 4+8)
	for i := 0; i < len(checksumBytes); i++ {
		checksumBytes[i] = dataLenBytes[i]
	}

	if checksum != crc32.ChecksumIEEE(checksumBytes) {
		return 0, errors.New("wrong checksum")
	}
	return dataLen, nil
}

func (this *Storage) read(offset uint64) (uint32, []byte, error) {
	fileIndex := offset >> 50
	offset = offset & 0x0000FFFFFFFFFFFF
	if fileIndex > uint64(len(this.files)-1) {
		return 0, nil, errors.New("wrong offset, index > open files")
	}
	file := this.files[fileIndex]

	file.RLock()
	defer file.RUnlock()

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
	fileIndex := uint32(id % uint32(len(this.files)))
	file := this.files[fileIndex]

	file.Lock()
	defer file.Unlock()

	currentOffset := file.offset
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return 0, "", err
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(len(b)))
	if err != nil {
		return 0, "", err
	}

	err = binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	if err != nil {
		return 0, "", err
	}
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, uint32(checksum))
	if err != nil {
		return 0, "", err
	}

	written, err := file.descriptor.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	written, err = file.descriptor.Write(b)
	if err != nil {
		panic(err)
	}
	file.offset += int64(written)

	return (uint64(fileIndex) << uint64(50)) | uint64(currentOffset), file.path, nil
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
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

func (this *MultiStore) scan(storageIdentifier string, cb func(uint32, []byte)) {
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
		os.Exit(1)
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

		dataLenRaw := make([]byte, 4)
		multiStore.scan(r.URL.Query().Get(storagePrefixKey), func(dataLen uint32, data []byte) {
			binary.LittleEndian.PutUint32(dataLenRaw, uint32(len(data)))
			w.Write(dataLenRaw)

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
		buf := bytes.NewReader(b)
		storagePrefix := r.URL.Query().Get(storagePrefixKey)
		for i := 0; i < len(b)/8; i++ {
			var offset uint64
			err = binary.Read(buf, binary.LittleEndian, &offset)
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
