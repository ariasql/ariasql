package wal

import (
	"ariasql/parser"
	"ariasql/storage/btree"
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"sync"
)

type WAL struct {
	// The file descriptor for the WAL file
	file *btree.Pager
	// The file path for the WAL file
	filePath string
	lock     *sync.Mutex
	// Every WAL contains ASTs to recover the database
}

func OpenWAL(filePath string, flags int, perm os.FileMode) (*WAL, error) {

	log.Println("done", filePath, flags, perm)
	wal, err := btree.OpenPager(filePath, flags, perm)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:     wal,
		filePath: filePath,
		lock:     &sync.Mutex{},
	}, nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Append(data []byte) error {


	w.lock.Lock()
	defer w.lock.Unlock()
	_, err := w.file.Write(data)
	if err != nil {
		log.Println(err.Error())
	}
	return err
}

type Entry struct {
	Statement *parser.Statement
}

// Encode ASTs to be written to the WAL file
func (w *WAL) Encode(stmt *parser.Statement) []byte {
	buff := make([]byte, 0)
	entry := &Entry{
		Statement: stmt,
	}

	enc := gob.NewEncoder(bytes.NewBuffer(buff))
	err := enc.Encode(entry)
	if err != nil {
		return nil
	}

	return buff
}

// Decode wal entries
func (w *WAL) Decode(data []byte) *parser.Statement {
	entry := &Entry{}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(entry)
	if err != nil {
		return nil
	}

	return entry.Statement
}

// Recover abstract syntax trees from the WAL file
func (w *WAL) Recover() []*parser.Statement {
	return nil
}
