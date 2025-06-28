package datastore

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("record not found")

const (
	outFileNamePrefix = "segment-"
	deleteMarker      = "__DELETE__"
)

type Db struct {
	out         *os.File
	outPath     string
	outOffset   int64
	dir         string
	segmentSize int64
	mu          sync.RWMutex
	index       map[string]int64
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		dir:         dir,
		index:       make(map[string]int64),
		segmentSize: segmentSize,
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := db.recover(); err != nil {
		return nil, err
	}
	return db, db.setOutFile()
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) recover() error {
	files, err := filepath.Glob(filepath.Join(db.dir, outFileNamePrefix+"*"))
	if err != nil {
		return err
	}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		var offset int64
		for {
			entry, size, err := readEntry(f)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if entry.value == deleteMarker {
				delete(db.index, entry.key)
			} else {
				db.index[entry.key] = offset
			}
			offset += size
		}
		f.Close()
	}
	return nil
}

func (db *Db) setOutFile() error {
	files, err := filepath.Glob(filepath.Join(db.dir, outFileNamePrefix+"*"))
	if err != nil {
		return err
	}
	lastIndex := 0
	if len(files) > 0 {
		lastFile := files[len(files)-1]
		parts := strings.Split(lastFile, "-")
		lastIndex, _ = strconv.Atoi(parts[len(parts)-1])
	}
	newName := filepath.Join(db.dir, outFileNamePrefix+strconv.Itoa(lastIndex+1))
	f, err := os.OpenFile(newName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	db.out = f
	db.outPath = newName
	db.outOffset = 0
	return nil
}

func readEntry(r io.Reader) (*entry, int64, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}
	keySize := binary.LittleEndian.Uint32(header[0:4])
	valueSize := binary.LittleEndian.Uint32(header[4:8])
	data := make([]byte, keySize+valueSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 0, err
	}
	e := &entry{
		key:   string(data[0:keySize]),
		value: string(data[keySize:]),
	}
	return e, int64(8 + keySize + valueSize), nil
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	offset, ok := db.index[key]
	db.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}
	files, err := filepath.Glob(filepath.Join(db.dir, outFileNamePrefix+"*"))
	if err != nil {
		return "", err
	}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return "", err
		}
		defer f.Close()
		stat, err := f.Stat()
		if err != nil {
			return "", err
		}
		if offset < stat.Size() {
			f.Seek(offset, 0)
			entry, _, err := readEntry(f)
			if err != nil {
				return "", err
			}
			return entry.value, nil
		}
	}
	return "", ErrNotFound
}

func (db *Db) Put(key, value string) error {
	e := entry{key: key, value: value}
	encoded := e.Encode()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.outOffset+int64(len(encoded)) > db.segmentSize {
		if err := db.mergeSegments(); err != nil {
			return err
		}
		if err := db.setOutFile(); err != nil {
			return err
		}
	}
	if _, err := db.out.Write(encoded); err != nil {
		return err
	}
	if value == deleteMarker {
		delete(db.index, key)
	} else {
		db.index[key] = db.outOffset
	}
	db.outOffset += int64(len(encoded))
	return nil
}

func (db *Db) Delete(key string) error {
	return db.Put(key, deleteMarker)
}

func (db *Db) mergeSegments() error {
	mergedPath := filepath.Join(db.dir, "merged")
	mergedFile, err := os.OpenFile(mergedPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer mergedFile.Close()
	newIndex := make(map[string]int64)
	var newOffset int64
	for key, offset := range db.index {
		files, err := filepath.Glob(filepath.Join(db.dir, outFileNamePrefix+"*"))
		if err != nil {
			return err
		}
		for _, file := range files {
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			stat, err := f.Stat()
			if err != nil {
				f.Close()
				return err
			}
			if offset < stat.Size() {
				f.Seek(offset, 0)
				entry, _, err := readEntry(f)
				if err != nil {
					f.Close()
					return err
				}
				if entry.key == key && entry.value != deleteMarker {
					encoded := entry.Encode()
					if _, err := mergedFile.Write(encoded); err != nil {
						f.Close()
						return err
					}
					newIndex[key] = newOffset
					newOffset += int64(len(encoded))
				}
				break
			}
			f.Close()
		}
	}
	if err := os.Remove(db.out.Name()); err != nil {
		return err
	}
	if err := mergedFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(mergedPath, db.outPath); err != nil {
		return err
	}
	db.index = newIndex
	return nil
}
