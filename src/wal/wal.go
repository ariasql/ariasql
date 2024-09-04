// Package wal
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

// WAL is a write-ahead log file
type WAL struct {
	// The file descriptor for the WAL file
	file *btree.Pager
	// The file path for the WAL file
	filePath string
	lock     *sync.Mutex
	// Every WAL contains ASTs to recover the database
}

// OpenWAL opens a new WAL file
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

// Close the WAL file
func (w *WAL) Close() error {
	return w.file.Close()
}

// Append data to the WAL file
func (w *WAL) Append(data []byte) error {

	w.lock.Lock()
	defer w.lock.Unlock()
	_, err := w.file.Write(data)
	if err != nil {
		log.Println(err.Error())
	}
	return err
}

// Entry is a single entry in the WAL file
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
