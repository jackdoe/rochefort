package main

import (
	"encoding/binary"
	"encoding/json"
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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	Tags   map[string]uint64
	Offset uint64
	File   string
}

type PostingsList struct {
	descriptor *os.File
	offset     uint64
}

func (this *PostingsList) newTermQuery() *Term {
	postings := make([]byte, this.offset)
	n, err := this.descriptor.ReadAt(postings, 0)
	if n != len(postings) && err != nil {
		postings = []byte{}
	}

	longed := make([]int64, len(postings)/8)
	j := 0
	for i := 0; i < len(postings); i += 8 {
		longed[j] = int64(binary.LittleEndian.Uint64(postings[i:]))
		j++
	}
	return NewTerm(longed)
}

type StoreItem struct {
	path       string
	root       string
	descriptor *os.File
	index      map[string]*PostingsList
	offset     uint64
	sync.RWMutex
}

var nonAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9_]+")

func sanitize(s string) string {
	return nonAlphaNumeric.ReplaceAllLiteralString(s, "")
}

func openAtEnd(filePath string) (*os.File, uint64) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	offset, err := f.Seek(0, 2)

	if err != nil {
		panic(err)
	}

	log.Printf("openning: %s with offset: %d", filePath, offset)
	return f, uint64(offset)
}

func NewStorage(root string) *StoreItem {
	os.MkdirAll(root, 0700)

	filePath := path.Join(root, "append.raw")
	f, offset := openAtEnd(filePath)

	si := &StoreItem{
		offset:     uint64(offset),
		path:       filePath,
		index:      map[string]*PostingsList{},
		descriptor: f,
		root:       root,
	}

	files, err := ioutil.ReadDir(root)
	if err != nil {
		panic(err)
	}
	for _, dirFile := range files {
		if strings.HasSuffix(dirFile.Name(), ".postings") {
			dot := strings.IndexRune(dirFile.Name(), '.')
			idxName := dirFile.Name()[:dot]
			si.CreatePostingsList(idxName)
		}
	}

	return si
}

func (this *StoreItem) GetPostingsList(name string) *PostingsList {
	name = sanitize(name)
	this.RLock()
	defer this.RUnlock()

	if p, ok := this.index[name]; ok {
		return p
	}
	return nil
}

func (this *StoreItem) CreatePostingsList(name string) *PostingsList {
	name = sanitize(name)
	this.RLock()

	if p, ok := this.index[name]; ok {
		this.RUnlock()
		return p
	}
	this.RUnlock()
	this.Lock()
	defer this.Unlock()

	if p, ok := this.index[name]; ok {
		return p
	}

	f, offset := openAtEnd(path.Join(this.root, fmt.Sprintf("%s.postings", name)))
	p := &PostingsList{
		descriptor: f,
		offset:     offset,
	}
	this.index[name] = p
	return p
}

func (this *StoreItem) stats() *Stats {
	out := &Stats{
		Tags:   make(map[string]uint64),
		Offset: this.offset,
		File:   this.path,
	}

	this.RLock()
	defer this.RUnlock()
	for name, index := range this.index {
		out.Tags[name] = index.offset / 8
	}

	return out
}

func (this *StoreItem) scan(cb func(uint64, []byte) bool) {
SCAN:
	for offset := uint64(0); offset < this.offset; {
		// this is lockless, which means we could read a header,
		// but the data might be incomplete

		dataLen, _, allocSize, err := readHeader(this.descriptor, offset)
		if err != nil {
			break SCAN
		}
		output := make([]byte, dataLen)
		_, err = this.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
		if err != nil {
			break SCAN
		}

		if !cb(offset, output) {
			break SCAN
		}

		offset += uint64(allocSize) + uint64(headerLen)
	}
}

func (this *StoreItem) ExecuteQuery(query Query, cb func(uint64, []byte) bool) {
	for query.Next() != NO_MORE {
		offset := uint64(query.GetDocId())
		dataLen, _, _, err := readHeader(this.descriptor, offset)
		if err != nil {
			break
		}

		output := make([]byte, dataLen)
		_, err = this.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
		if err != nil {
			break
		}

		if !cb(offset, output) {
			break
		}
	}
}

const headerLen = 4 + 8 + 4 + 4

func readHeader(file *os.File, offset uint64) (uint32, uint64, uint32, error) {
	headerBytes := make([]byte, headerLen)
	_, err := file.ReadAt(headerBytes, int64(offset))
	if err != nil {
		return 0, 0, 0, err
	}
	dataLen := binary.LittleEndian.Uint32(headerBytes[0:])
	nextBlock := binary.LittleEndian.Uint64(headerBytes[4:])
	allocSize := binary.LittleEndian.Uint32(headerBytes[12:])
	checksum := binary.LittleEndian.Uint32(headerBytes[16:])
	computedChecksum := crc(headerBytes[0:16])
	if checksum != computedChecksum {
		return 0, 0, 0, errors.New(fmt.Sprintf("wrong checksum got: %d, expected: %d", computedChecksum, checksum))
	}
	return dataLen, nextBlock, allocSize, nil
}

func (this *StoreItem) writeHeader(currentOffset uint64, dataLen uint32, nextBlockOffset uint64, allocSize uint32) {
	header := make([]byte, headerLen)

	binary.LittleEndian.PutUint32(header[0:], uint32(dataLen))
	binary.LittleEndian.PutUint64(header[4:], uint64(0))
	binary.LittleEndian.PutUint32(header[12:], allocSize)

	checksum := crc(header[0:16])
	binary.LittleEndian.PutUint32(header[16:], checksum)

	_, err := this.descriptor.WriteAt(header, int64(currentOffset))

	if err != nil {
		panic(err)
	}
}

func (this *StoreItem) appendPostings(name string, value uint64) {
	p := this.CreatePostingsList(name)

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)

	// add it to the end
	offset := atomic.AddUint64(&p.offset, uint64(8)) - 8
	p.descriptor.WriteAt(data, int64(offset))
}

func (this *StoreItem) read(offset uint64) (uint32, []byte, error) {
	// lockless read
	dataLen, _, _, err := readHeader(this.descriptor, offset)
	if err != nil {
		return 0, nil, err
	}

	output := make([]byte, dataLen)
	_, err = this.descriptor.ReadAt(output, int64(offset)+int64(headerLen))
	if err != nil {
		return 0, nil, err
	}
	return dataLen, output, nil
}

func (this *StoreItem) append(allocSize uint32, data io.Reader) (uint64, error) {
	dataRaw, err := ioutil.ReadAll(data)
	if err != nil {
		return 0, err
	}

	if len(dataRaw) > int(allocSize) {
		allocSize = uint32(len(dataRaw))
	}

	offset := atomic.AddUint64(&this.offset, uint64(allocSize+headerLen))

	currentOffset := offset - uint64(allocSize+headerLen)
	_, err = this.descriptor.WriteAt(dataRaw, int64(currentOffset+headerLen))
	if err != nil {
		panic(err)
	}

	this.writeHeader(currentOffset, uint32(len(dataRaw)), 0, allocSize)

	return currentOffset, nil
}

func (this *StoreItem) modify(offset uint64, pos int32, data io.Reader) error {
	dataRaw, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}

	oldDataLen, _, allocSize, err := readHeader(this.descriptor, offset)
	if err != nil {
		return err
	}

	if pos < 0 {
		pos = int32(oldDataLen)
	}

	end := uint32(pos) + uint32(len(dataRaw))
	if end > allocSize {
		return errors.New("pos+len > allocSize")
	}

	_, err = this.descriptor.WriteAt(dataRaw, int64(offset+uint64(headerLen)+uint64(pos)))
	if err != nil {
		panic(err)
	}

	if end > oldDataLen {
		// need to recompute the header
		this.writeHeader(offset, end, 0, allocSize)
	}
	return nil
}

func crc(b []byte) uint32 {
	return uint32(metro.Hash64(b, 0) >> uint64(32))
}

type MultiStore struct {
	stores map[string]*StoreItem
	root   string
	sync.RWMutex
}

func (this *MultiStore) find(storageIdentifier string) *StoreItem {
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
			storage = NewStorage(path.Join(this.root, storageIdentifier))
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
		storage.descriptor.Close()
		storage.Lock()
		for name, i := range storage.index {
			log.Printf("closing: %s/%s.postings", storage.root, name)
			i.descriptor.Close()
		}
		storage.index = make(map[string]*PostingsList)
		storage.Unlock()
		log.Printf("closing: %s", storage.path)
	}
	delete(this.stores, storageIdentifier)
}

func (this *MultiStore) modify(storageIdentifier string, offset uint64, pos int32, data io.Reader) error {
	return this.find(storageIdentifier).modify(offset, pos, data)
}

func (this *MultiStore) stats(storageIdentifier string) *Stats {
	return this.find(storageIdentifier).stats()
}

func (this *MultiStore) append(storageIdentifier string, allocSize uint32, data io.Reader) (uint64, error) {
	return this.find(storageIdentifier).append(allocSize, data)
}

func (this *MultiStore) read(storageIdentifier string, offset uint64) (uint32, []byte, error) {
	return this.find(storageIdentifier).read(offset)
}

func (this *MultiStore) scan(storageIdentifier string, cb func(uint64, []byte) bool) {
	this.find(storageIdentifier).scan(cb)
}

func (this *MultiStore) ExecuteQuery(storageIdentifier string, query Query, cb func(uint64, []byte) bool) {
	this.find(storageIdentifier).ExecuteQuery(query, cb)
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

func getTags(tags string) []string {
	if tags == "" {
		return []string{}
	}
	splitted := strings.Split(tags, ",")
	out := []string{}
	for _, s := range splitted {
		if s != "" {
			out = append(out, s)
		}
	}
	return out

}

const namespaceKey = "namespace"
const posKey = "pos"
const allocSizeKey = "allocSize"
const offsetKey = "offset"
const tagsKey = "tags"

func main() {
	var pbind = flag.String("bind", ":8000", "address to bind to")
	var proot = flag.String("root", "/tmp/rochefort", "root directory")
	var pquiet = flag.Bool("quiet", false, "dont print any log messages")
	flag.Parse()

	multiStore := &MultiStore{
		stores: make(map[string]*StoreItem),
		root:   *proot,
	}

	os.MkdirAll(*proot, 0700)
	namespaces, err := ioutil.ReadDir(*proot)
	if err != nil {
		panic(err)
	}
	// open all files in the namespace
NAMESPACE:
	for _, namespace := range namespaces {
		if namespace.IsDir() {
			files, err := ioutil.ReadDir(path.Join(*proot, namespace.Name()))
			if err == nil {
				for _, file := range files {
					if strings.HasSuffix(file.Name(), ".raw") {
						multiStore.find(namespace.Name())
						continue NAMESPACE
					}
				}
			}

		}
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("\nReceived an interrupt, stopping services...\n")
		multiStore.Lock() // dont unlock it
		for _, storage := range multiStore.stores {
			storage.Lock() // dont unlock it
			storage.descriptor.Close()
			for name, i := range storage.index {
				log.Printf("closing: %s/%s.postings", storage.root, name)
				i.descriptor.Close()
			}
			log.Printf("closing: %s", storage.path)
		}
		os.Exit(0)

	}()

	http.HandleFunc("/close", func(w http.ResponseWriter, r *http.Request) {
		multiStore.close(r.URL.Query().Get(namespaceKey))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"success\":true}"))
	})

	http.HandleFunc("/modify", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseUint(r.URL.Query().Get(offsetKey), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			pos, err := strconv.ParseInt(r.URL.Query().Get(posKey), 10, 32)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
			} else {
				err := multiStore.modify(r.URL.Query().Get(namespaceKey), offset, int32(pos), r.Body)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
				} else {
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte("{\"success\":true}"))
				}
			}
		}
	})

	http.HandleFunc("/append", func(w http.ResponseWriter, r *http.Request) {
		allocSize := uint64(0)
		if r.URL.Query().Get(allocSizeKey) != "" {
			allocSizeInput, err := strconv.ParseUint(r.URL.Query().Get(allocSizeKey), 10, 64)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			} else {
				allocSize = allocSizeInput
			}
		}
		store := multiStore.find(r.URL.Query().Get(namespaceKey))
		offset, err := store.append(uint32(allocSize), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			tags := getTags(r.URL.Query().Get(tagsKey))
			for _, t := range tags {
				store.appendPostings(t, offset)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(fmt.Sprintf("{\"offset\":%d}", offset)))
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseUint(r.URL.Query().Get(offsetKey), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			_, data, err := multiStore.read(r.URL.Query().Get(namespaceKey), offset)
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
		cb := func(offset uint64, data []byte) bool {
			binary.LittleEndian.PutUint32(header[0:], uint32(len(data)))
			binary.LittleEndian.PutUint64(header[4:], offset)

			_, err := w.Write(header)
			if err != nil {
				return false
			}
			_, err = w.Write(data)
			if err != nil {
				return false
			}
			return true
		}
		multiStore.scan(r.URL.Query().Get(namespaceKey), cb)
	})

	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")

		defer r.Body.Close()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		var decoded map[string]interface{}
		err = json.Unmarshal(body, &decoded)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		stored := multiStore.find(r.URL.Query().Get(namespaceKey))

		query, err := fromJSON(stored, decoded)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		header := make([]byte, 12)
		cb := func(offset uint64, data []byte) bool {
			binary.LittleEndian.PutUint32(header[0:], uint32(len(data)))
			binary.LittleEndian.PutUint64(header[4:], offset)

			_, err := w.Write(header)
			if err != nil {
				return false
			}
			_, err = w.Write(data)
			if err != nil {
				return false
			}
			return true
		}
		stored.ExecuteQuery(query, cb)
	})

	http.HandleFunc("/stat", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		stats := multiStore.stats(r.URL.Query().Get(namespaceKey))
		b, err := json.Marshal(stats)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(b)
		}
	})

	http.HandleFunc("/getMulti", func(w http.ResponseWriter, r *http.Request) {
		dataLenRaw := make([]byte, 4)
		defer r.Body.Close()
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("read: %s", err.Error())))
			return
		}

		namespace := r.URL.Query().Get(namespaceKey)

		if len(b)%8 != 0 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("incomplete read: %d is not multiple of 8", len(b))))
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		for i := 0; i < len(b); i += 8 {
			offset := binary.LittleEndian.Uint64(b[i:])
			_, data, err := multiStore.read(namespace, offset)

			// XXX: we ignore the error on purpose
			// as the storage is not fsyncing, it could very well lose some updates
			// also the data is barely checksummed, so might very well be corrupted
			if err == nil {
				binary.LittleEndian.PutUint32(dataLenRaw, uint32(len(data)))
				_, err = w.Write(dataLenRaw)
				if err != nil {
					return
				}
				_, err = w.Write(data)
				if err != nil {
					return
				}
			}
		}
	})

	if !*pquiet {
		log.Printf("starting http server on %s", *pbind)
		err := http.ListenAndServe(*pbind, Log(http.DefaultServeMux))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		err := http.ListenAndServe(*pbind, http.DefaultServeMux)
		if err != nil {
			log.Fatal(err)
		}

	}
}
