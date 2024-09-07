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
	"ariasql/catalog"
	"ariasql/parser"
	"ariasql/storage/btree"
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"sync"
)

// WAL is a write-ahead log file
type WAL struct {
	// The file descriptor for the WAL file
	file *btree.Pager
	// The file path for the WAL file
	FilePath string
	lock     *sync.Mutex // Lock for the WAL file
	// Every WAL contains ASTs to recover the database
}

// OpenWAL opens a new WAL file
func OpenWAL(filePath string, flags int, perm os.FileMode) (*WAL, error) {
	wal, err := btree.OpenPager(filePath, flags, perm)
	if err != nil {
		return nil, err
	}

	gob.Register(&parser.CreateDatabaseStmt{})
	gob.Register(&parser.InsertStmt{})
	gob.Register(&parser.CreateTableStmt{})
	gob.Register(&catalog.TableSchema{})
	gob.Register(&parser.DropTableStmt{})
	gob.Register(&parser.SelectStmt{})
	gob.Register(&parser.UpdateStmt{})
	gob.Register(&parser.DeleteStmt{})
	gob.Register(&parser.CreateIndexStmt{})
	gob.Register(&parser.DropIndexStmt{})
	gob.Register(&parser.UseStmt{})
	gob.Register(&parser.Literal{})
	gob.Register(&parser.Identifier{})
	gob.Register([]*parser.Identifier{})
	gob.Register([][]*parser.Literal{})

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
		return err
	}

	return nil
}

// Encode ASTs to be written to the WAL file
func (w *WAL) Encode(stmt interface{}) []byte {

	buff := bytes.NewBuffer([]byte{})

	switch stmt.(type) {

	case *parser.CreateDatabaseStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CreateTableStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt.(*parser.CreateTableStmt))
		if err != nil {
			return nil
		}
	case *parser.InsertStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}
	case *parser.DropTableStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.SelectStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.UpdateStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DeleteStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CreateIndexStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DropIndexStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.UseStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	default:
		return nil
	}

	return buff.Bytes()
}

// Decode wal entries
func (w *WAL) Decode(data []byte) interface{} {

	stmtTypes := []interface{}{
		&parser.InsertStmt{},
		&parser.CreateDatabaseStmt{},
		&parser.CreateTableStmt{},
		&parser.DropTableStmt{},
		&parser.SelectStmt{},
		&parser.UpdateStmt{},
		&parser.DeleteStmt{},
		&parser.CreateIndexStmt{},
		&parser.DropIndexStmt{},
		&parser.UseStmt{},
	}

	for _, stmtType := range stmtTypes {

		switch stmtType.(type) {
		case *parser.CreateDatabaseStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.CreateDatabaseStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			return stmt
		case *parser.CreateTableStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.CreateTableStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			return stmt
		case *parser.DropTableStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.DropTableStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			return stmt
		case *parser.InsertStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.InsertStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.Values == nil {
				continue
			}

			return stmt
		case *parser.CreateIndexStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.CreateIndexStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.IndexName == nil {
				continue
			}

			return stmt
		case *parser.DropIndexStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.DropIndexStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.IndexName == nil {
				continue
			}

			return stmt
		case *parser.CreateUserStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.CreateUserStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.Username == nil {
				continue
			}

			return stmt
		case *parser.DropUserStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.DropUserStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.Username == nil {
				continue
			}

			return stmt
		case *parser.GrantStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.GrantStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.PrivilegeDefinition == nil {
				continue
			}

			return stmt
		case *parser.RevokeStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.RevokeStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.PrivilegeDefinition == nil {
				continue
			}

			return stmt
		case *parser.UseStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.UseStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.DatabaseName == nil {
				continue
			}

			return stmt
		case *parser.AlterUserStmt:
			dec := gob.NewDecoder(bytes.NewBuffer(data))
			stmt := &parser.AlterUserStmt{}
			err := dec.Decode(stmt)
			if err != nil {
				continue
			}

			if stmt.Username == nil {
				continue
			}

			return stmt

		}

	}

	return nil
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
			switch stmt := stmt.(type) {
			case *parser.CreateDatabaseStmt:
				stmts = append(stmts, stmt)
			case *parser.CreateTableStmt:
				stmts = append(stmts, stmt)
			case *parser.DropTableStmt:
				stmts = append(stmts, stmt)
			case *parser.InsertStmt:
				stmts = append(stmts, stmt)
			case *parser.SelectStmt:
				stmts = append(stmts, stmt)
			case *parser.UpdateStmt:
				stmts = append(stmts, stmt)
			case *parser.DeleteStmt:
				stmts = append(stmts, stmt)
			case *parser.CreateIndexStmt:
				stmts = append(stmts, stmt)
			case *parser.DropIndexStmt:
				stmts = append(stmts, stmt)
			case *parser.UseStmt:
				stmts = append(stmts, stmt)
			case *parser.AlterUserStmt:
				stmts = append(stmts, stmt)
			case *parser.CreateUserStmt:
				stmts = append(stmts, stmt)
			case *parser.DropUserStmt:
				stmts = append(stmts, stmt)
			case *parser.GrantStmt:
				stmts = append(stmts, stmt)
			case *parser.RevokeStmt:
				stmts = append(stmts, stmt)

			default:
				return nil, errors.New("unknown statement type found in WAL")
			}
		}

	}

	return stmts, nil
}
