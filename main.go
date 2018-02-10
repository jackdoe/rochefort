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

func (this *Storage) read(sid string, offset int64) ([]byte, error) {
	id := hash(sid)
	file := this.files[id%uint32(len(this.files))]

	file.RLock()
	defer file.RUnlock()

	dataLenBytes := make([]byte, 4+8+4)
	_, err := file.descriptor.ReadAt(dataLenBytes, offset)
	if err != nil {
		return nil, err
	}

	var dataLen uint32
	var timeNano uint64
	var checksum uint32
	buf := bytes.NewReader(dataLenBytes)
	err = binary.Read(buf, binary.LittleEndian, &dataLen)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &timeNano)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.LittleEndian, &checksum)
	if err != nil {
		return nil, err
	}
	checksumBytes := make([]byte, 4+8)
	for i := 0; i < len(checksumBytes); i++ {
		checksumBytes[i] = dataLenBytes[i]
	}

	if checksum != crc32.ChecksumIEEE(checksumBytes) {
		return nil, errors.New("wrong checksum")
	}

	output := make([]byte, dataLen)
	_, err = file.descriptor.ReadAt(output, offset+int64(len(dataLenBytes)))
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (this *Storage) append(sid string, data io.Reader) (int64, string) {
	id := hash(sid)
	file := this.files[id%uint32(len(this.files))]

	file.Lock()
	defer file.Unlock()

	currentOffset := file.offset
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return -1, ""
	}

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(len(b)))
	if err != nil {
		return -1, ""
	}

	err = binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	if err != nil {
		return -1, ""
	}
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	err = binary.Write(buf, binary.LittleEndian, uint32(checksum))
	if err != nil {
		return -1, ""
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

	return currentOffset, file.path
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

func (this *MultiStore) append(storageIdentifier, sid string, data io.Reader) (int64, string) {
	return this.find(storageIdentifier).append(sid, data)
}

func (this *MultiStore) read(storageIdentifier, sid string, offset int64) ([]byte, error) {
	return this.find(storageIdentifier).read(sid, offset)
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

func main() {
	var pnBuckets = flag.Int("buckets", 128, "number of files to open")
	var pbind = flag.String("bind", ":8000", "address to bind to")
	var proot = flag.String("root", "/tmp", "root directory")
	flag.Parse()

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
		multiStore.close(r.URL.Query().Get("storagePrefix"))
		w.Write([]byte("{\"success\":true}"))
	})

	http.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		offset, file := multiStore.append(r.URL.Query().Get("storagePrefix"), r.URL.Query().Get("id"), r.Body)
		w.Write([]byte(fmt.Sprintf("{\"offset\":%d,\"file\":\"%s\"}", offset, file)))
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			data, err := multiStore.read(r.URL.Query().Get("storagePrefix"), r.URL.Query().Get("id"), offset)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			} else {
				w.Write(data)
			}
		}
	})

	http.HandleFunc("/getMulti", func(w http.ResponseWriter, r *http.Request) {
		dataLenRaw := make([]byte, 4)
		defer r.Body.Close()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			binary.LittleEndian.PutUint32(dataLenRaw, 0)
			w.Write([]byte(fmt.Sprintf("read: %s", err.Error())))
			return
		}
		buf := bytes.NewReader(b)
		for i := 0; i < len(b)/8; i++ {
			var offset uint64
			err = binary.Read(buf, binary.LittleEndian, &offset)
			data, err := multiStore.read(r.URL.Query().Get("storagePrefix"), r.URL.Query().Get("id"), int64(offset))

			// XXX: we ignore the error on purpose
			// as the storage is not fsyncing, it could very well lose some updates
			// also the data is not checksummed, so might very well be corrupted
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
