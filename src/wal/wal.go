// Package wal
// Copyright (C) AriaSQL
// Author(s): Alex Gaetano Padula
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
	FilePath string
	lock     *sync.Mutex
	// Every WAL contains ASTs to recover the database
}

// OpenWAL opens a new WAL file
func OpenWAL(filePath string, flags int, perm os.FileMode) (*WAL, error) {
	gob.Register(parser.CreateDatabaseStmt{})
	gob.Register(parser.CreateTableStmt{})
	gob.Register(parser.DropTableStmt{})
	gob.Register(parser.InsertStmt{})
	gob.Register(parser.SelectStmt{})
	gob.Register(parser.UpdateStmt{})
	gob.Register(parser.DeleteStmt{})
	gob.Register(parser.CreateIndexStmt{})
	gob.Register(parser.DropIndexStmt{})
	gob.Register(parser.UseStmt{})

	wal, err := btree.OpenPager(filePath, flags, perm)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:     wal,
		FilePath: filePath,
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
	Statement interface{}
}

// Encode ASTs to be written to the WAL file
func (w *WAL) Encode(stmt interface{}) []byte {
	buff := bytes.NewBuffer([]byte{})
	var entry *Entry
	switch stmt.(type) {
	case *parser.CreateDatabaseStmt:
		entry = &Entry{
			Statement: stmt.(*parser.CreateDatabaseStmt),
		}
	case *parser.CreateTableStmt:
		entry = &Entry{
			Statement: stmt.(*parser.CreateTableStmt),
		}
	case *parser.DropTableStmt:
		entry = &Entry{
			Statement: stmt.(*parser.DropTableStmt),
		}
	case *parser.InsertStmt:
		entry = &Entry{
			Statement: stmt.(*parser.InsertStmt),
		}
	case *parser.SelectStmt:
		entry = &Entry{
			Statement: stmt.(*parser.SelectStmt),
		}
	case *parser.UpdateStmt:
		entry = &Entry{
			Statement: stmt.(*parser.UpdateStmt),
		}
	case *parser.DeleteStmt:
		entry = &Entry{
			Statement: stmt.(*parser.DeleteStmt),
		}
	case *parser.CreateIndexStmt:
		entry = &Entry{
			Statement: stmt.(*parser.CreateIndexStmt),
		}
	case *parser.DropIndexStmt:
		entry = &Entry{
			Statement: stmt.(*parser.DropIndexStmt),
		}
	case *parser.UseStmt:
		entry = &Entry{
			Statement: stmt.(*parser.UseStmt),
		}
	default:
		return nil
	}

	enc := gob.NewEncoder(buff)
	err := enc.Encode(entry)
	if err != nil {
		return nil
	}

	return buff.Bytes()
}

// Decode wal entries
func (w *WAL) Decode(data []byte) interface{} {
	entry := &Entry{}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(entry)
	if err != nil {
		log.Println(err.Error())
		return nil
	}

	return entry.Statement
}

// RecoverASTs Recover abstract syntax trees from the WAL file
func (w *WAL) RecoverASTs() ([]interface{}, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	stmts := make([]interface{}, 0)

	pages := w.file.Count()

	for i := 0; i < int(pages); i++ {
		data, err := w.file.GetPage(int64(i))
		if err != nil {
			return nil, err
		}

		stmt := w.Decode(data)
		if stmt != nil {
			stmts = append(stmts, stmt)
		}

	}

	return stmts, nil
}
