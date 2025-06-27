package datastore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	outFileName   = "current-data"
	segmentPrefix = "segment-"
	deleteMarker  = "__DELETE__"
	segmentSize   = 1024
)

var ErrNotFound = fmt.Errorf("record does not exist")

type valuePos struct {
	segmentId int
	offset    int64
}

type Db struct {
	dir           string
	activeSegment *os.File
	segmentId     int
	segments      map[int]*os.File
	index         map[string]valuePos
	mu            sync.RWMutex
	putCh         chan *entry
	quitCh        chan struct{}
}

func Open(dir string) (*Db, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}

	db := &Db{
		dir:      dir,
		segments: make(map[int]*os.File),
		index:    make(map[string]valuePos),
		putCh:    make(chan *entry),
		quitCh:   make(chan struct{}),
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var segmentIds []int
	for _, file := range files {
		if strings.HasPrefix(file.Name(), segmentPrefix) {
			idStr := strings.TrimPrefix(file.Name(), segmentPrefix)
			id, err := strconv.Atoi(idStr)
			if err == nil {
				segmentIds = append(segmentIds, id)
			}
		}
	}
	sort.Ints(segmentIds)

	for _, id := range segmentIds {
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%s%d", segmentPrefix, id)), os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		db.segments[id] = f
		db.recover(f, id)
	}

	if len(segmentIds) > 0 {
		db.segmentId = segmentIds[len(segmentIds)-1] + 1
	} else {
		db.segmentId = 0
	}

	outPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	db.activeSegment = f
	db.recover(f, db.segmentId)

	go db.putLoop()
	go db.compactionLoop()

	return db, nil
}

func (db *Db) recover(f *os.File, segmentId int) {
	var offset int64
	for {
		header := make([]byte, 8)
		n, err := f.ReadAt(header, offset)
		if err == io.EOF || n < 8 {
			break
		}
		kl := binary.LittleEndian.Uint32(header)
		vl := binary.LittleEndian.Uint32(header[4:])

		data := make([]byte, kl+vl+8)
		_, err = f.ReadAt(data, offset)
		if err == io.EOF {
			break
		}

		e := &entry{}
		e.Decode(data)

		if e.value == deleteMarker {
			delete(db.index, e.key)
		} else {
			db.index[e.key] = valuePos{segmentId: segmentId, offset: offset}
		}
		offset += int64(8 + kl + vl)
	}
}

func (db *Db) Close() error {
	close(db.quitCh)
	time.Sleep(100 * time.Millisecond)

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, f := range db.segments {
		f.Close()
	}
	return db.activeSegment.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	pos, ok := db.index[key]
	db.mu.RUnlock()

	if !ok {
		return "", ErrNotFound
	}

	var segmentFile *os.File
	if pos.segmentId == db.segmentId {
		f, err := os.Open(db.activeSegment.Name())
		if err != nil {
			return "", err
		}
		defer f.Close()
		segmentFile = f
	} else {
		segmentFile = db.segments[pos.segmentId]
	}

	header := make([]byte, 8)
	_, err := segmentFile.ReadAt(header, pos.offset)
	if err != nil {
		return "", err
	}
	kl := binary.LittleEndian.Uint32(header)
	vl := binary.LittleEndian.Uint32(header[4:])

	data := make([]byte, 8+kl+vl)
	_, err = segmentFile.ReadAt(data, pos.offset)
	if err != nil {
		return "", err
	}

	e := &entry{}
	e.Decode(data)

	if e.value == deleteMarker {
		return "", ErrNotFound
	}
	return e.value, nil
}

func (db *Db) Put(key, value string) error {
	e := &entry{key, value}
	db.putCh <- e
	return nil
}

func (db *Db) Delete(key string) error {
	return db.Put(key, deleteMarker)
}

func (db *Db) putLoop() {
	for {
		select {
		case <-db.quitCh:
			return
		case e := <-db.putCh:
			db.performPut(e.key, e.value)
		}
	}
}

func (db *Db) performPut(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	fi, err := db.activeSegment.Stat()
	if err != nil {
		return
	}
	offset := fi.Size()

	e := &entry{key, value}
	data := e.Encode()
	_, err = db.activeSegment.Write(data)
	if err != nil {
		return
	}

	if value == deleteMarker {
		delete(db.index, key)
	} else {
		db.index[key] = valuePos{segmentId: db.segmentId, offset: offset}
	}

	if fi.Size() >= segmentSize {
		db.activeSegment.Close()
		oldPath := filepath.Join(db.dir, outFileName)
		newPath := filepath.Join(db.dir, fmt.Sprintf("%s%d", segmentPrefix, db.segmentId))
		os.Rename(oldPath, newPath)

		f, _ := os.OpenFile(oldPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		db.activeSegment = f

		segmentFile, _ := os.OpenFile(newPath, os.O_RDONLY, 0666)
		db.segments[db.segmentId] = segmentFile

		db.segmentId++
	}
}

func (db *Db) compactionLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-db.quitCh:
			return
		case <-ticker.C:
			db.performCompaction()
		}
	}
}

func (db *Db) performCompaction() {
	db.mu.Lock()
	if len(db.segments) < 2 {
		db.mu.Unlock()
		return
	}

	ids := make([]int, 0, len(db.segments))
	for id := range db.segments {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	if len(ids) < 2 {
		db.mu.Unlock()
		return
	}
	id1 := ids[0]
	id2 := ids[1]

	segment1Path := db.segments[id1].Name()
	segment2Path := db.segments[id2].Name()
	db.mu.Unlock()

	tmpPath := segment1Path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return
	}

	compactIndex := make(map[string]string)

	db.readSegment(segment1Path, compactIndex)
	db.readSegment(segment2Path, compactIndex)

	for key, value := range compactIndex {
		if value != deleteMarker {
			e := entry{key, value}
			tmpFile.Write(e.Encode())
		}
	}
	tmpFile.Close()

	os.Rename(tmpPath, segment1Path)

	db.mu.Lock()
	defer db.mu.Unlock()

	db.segments[id2].Close()
	os.Remove(segment2Path)
	delete(db.segments, id2)

	db.segments[id1].Close()
	mergedFile, _ := os.OpenFile(segment1Path, os.O_RDONLY, 0666)
	db.segments[id1] = mergedFile

	db.index = make(map[string]valuePos)
	for id, f := range db.segments {
		db.recover(f, id)
	}
	db.recover(db.activeSegment, db.segmentId)

	fmt.Printf("Compaction complete: merged %d and %d into %d\n", id1, id2, id1)
}

func (db *Db) readSegment(path string, idx map[string]string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	var offset int64
	for {
		header := make([]byte, 8)
		n, err := f.ReadAt(header, offset)
		if err == io.EOF || n < 8 {
			break
		}
		kl := binary.LittleEndian.Uint32(header)
		vl := binary.LittleEndian.Uint32(header[4:])

		data := make([]byte, 8+kl+vl)
		_, err = f.ReadAt(data, offset)
		if err == io.EOF {
			break
		}
		e := &entry{}
		e.Decode(data)

		if e.value == deleteMarker {
			delete(idx, e.key)
		} else {
			idx[e.key] = e.value
		}

		offset += int64(8 + kl + vl)
	}
}
