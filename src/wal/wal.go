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
	gob.Register(&parser.UpdateStmt{})
	gob.Register(&parser.DeleteStmt{})
	gob.Register(&parser.CreateIndexStmt{})
	gob.Register(&parser.DropIndexStmt{})
	gob.Register(&parser.UseStmt{})
	gob.Register(&parser.Literal{})
	gob.Register(&parser.Identifier{})
	gob.Register([]*parser.Identifier{})
	gob.Register([][]*parser.Literal{})
	gob.Register(&parser.CreateProcedureStmt{})
	gob.Register(&parser.DropProcedureStmt{})
	gob.Register(&parser.CreateUserStmt{})
	gob.Register(&parser.DropUserStmt{})
	gob.Register(&parser.RevokeStmt{})
	gob.Register(&parser.GrantStmt{})
	gob.Register(&parser.AlterUserStmt{})
	gob.Register(&parser.ExecStmt{})
	gob.Register(&parser.DeallocateStmt{})
	gob.Register(&parser.WhileStmt{})
	gob.Register(&parser.IfStmt{})
	gob.Register(&parser.BeginEndBlock{})
	gob.Register(&parser.ElseIfStmt{})
	gob.Register(&parser.OpenStmt{})
	gob.Register(&parser.FetchStmt{})
	gob.Register(&parser.PrintStmt{})
	gob.Register(&parser.CloseStmt{})
	gob.Register(&parser.ExitStmt{})
	gob.Register(&parser.BreakStmt{})
	gob.Register(&parser.ReturnStmt{})
	gob.Register(&parser.Procedure{})
	gob.Register(&parser.Variable{})
	gob.Register(&parser.DeclareStmt{})
	gob.Register(&parser.ConcatFunc{})
	gob.Register(&parser.SetStmt{})
	gob.Register(&parser.ElseClause{})
	gob.Register(&parser.CaseExpr{})
	gob.Register(&parser.SubstrFunc{})
	gob.Register(&parser.TrimFunc{})
	gob.Register(&parser.LengthFunc{})
	gob.Register(&parser.PositionFunc{})
	gob.Register(&parser.RoundFunc{})
	gob.Register(&parser.ReverseFunc{})
	gob.Register(&parser.CoalesceFunc{})
	gob.Register(&parser.CastFunc{})
	gob.Register(&parser.LowerFunc{})
	gob.Register(&parser.UpperFunc{})
	gob.Register(&parser.ProcedureStmt{})
	gob.Register(&parser.Parameter{})
	gob.Register(&parser.PrivilegeDefinition{})
	gob.Register(&parser.BeginStmt{})
	gob.Register(&parser.CommitStmt{})
	gob.Register(&parser.RollbackStmt{})

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

	case *parser.AlterUserStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CreateUserStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DropUserStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.GrantStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.RevokeStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ExecStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DeallocateStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.WhileStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.IfStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.BeginEndBlock:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ElseIfStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.OpenStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.FetchStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.PrintStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CloseStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ExitStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.BreakStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ReturnStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.Procedure:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.Variable:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DeclareStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ConcatFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.SetStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ElseClause:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CaseExpr:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.SubstrFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.TrimFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.LengthFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.PositionFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.RoundFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ReverseFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CoalesceFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CastFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.LowerFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.UpperFunc:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.ProcedureStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.Parameter:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.PrivilegeDefinition:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.BeginStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CommitStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.RollbackStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.CreateProcedureStmt:
		enc := gob.NewEncoder(buff)
		err := enc.Encode(stmt)
		if err != nil {
			return nil
		}

	case *parser.DropProcedureStmt:
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
		&parser.UpdateStmt{},
		&parser.DeleteStmt{},
		&parser.CreateIndexStmt{},
		&parser.DropIndexStmt{},
		&parser.UseStmt{},
		&parser.AlterUserStmt{},
		&parser.CreateUserStmt{},
		&parser.DropUserStmt{},
		&parser.GrantStmt{},
		&parser.RevokeStmt{},
		&parser.ExecStmt{},
		&parser.DeallocateStmt{},
		&parser.WhileStmt{},
		&parser.IfStmt{},
		&parser.BeginEndBlock{},
		&parser.ElseIfStmt{},
		&parser.OpenStmt{},
		&parser.FetchStmt{},
		&parser.PrintStmt{},
		&parser.CloseStmt{},
		&parser.ExitStmt{},
		&parser.BreakStmt{},
		&parser.ReturnStmt{},
		&parser.Procedure{},
		&parser.Variable{},
		&parser.DeclareStmt{},
		&parser.ConcatFunc{},
		&parser.SetStmt{},
		&parser.ElseClause{},
		&parser.CaseExpr{},
		&parser.SubstrFunc{},
		&parser.TrimFunc{},
		&parser.LengthFunc{},
		&parser.PositionFunc{},
		&parser.RoundFunc{},
		&parser.ReverseFunc{},
		&parser.CoalesceFunc{},
		&parser.CastFunc{},
		&parser.LowerFunc{},
		&parser.UpperFunc{},
		&parser.ProcedureStmt{},
		&parser.Parameter{},
		&parser.PrivilegeDefinition{},
		&parser.BeginStmt{},
		&parser.CommitStmt{},
		&parser.RollbackStmt{},
		&parser.CreateProcedureStmt{},
		&parser.DropProcedureStmt{},
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
			case *parser.ExecStmt:
				stmts = append(stmts, stmt)
			case *parser.DeallocateStmt:
				stmts = append(stmts, stmt)
			case *parser.WhileStmt:
				stmts = append(stmts, stmt)
			case *parser.IfStmt:
				stmts = append(stmts, stmt)
			case *parser.BeginEndBlock:
				stmts = append(stmts, stmt)
			case *parser.ElseIfStmt:
				stmts = append(stmts, stmt)
			case *parser.OpenStmt:
				stmts = append(stmts, stmt)
			case *parser.FetchStmt:
				stmts = append(stmts, stmt)
			case *parser.PrintStmt:
				stmts = append(stmts, stmt)
			case *parser.CloseStmt:
				stmts = append(stmts, stmt)
			case *parser.ExitStmt:
				stmts = append(stmts, stmt)
			case *parser.BreakStmt:
				stmts = append(stmts, stmt)
			case *parser.ReturnStmt:
				stmts = append(stmts, stmt)
			case *parser.Procedure:
				stmts = append(stmts, stmt)
			case *parser.Variable:
				stmts = append(stmts, stmt)
			case *parser.DeclareStmt:
				stmts = append(stmts, stmt)
			case *parser.ConcatFunc:
				stmts = append(stmts, stmt)
			case *parser.SetStmt:
				stmts = append(stmts, stmt)
			case *parser.ElseClause:
				stmts = append(stmts, stmt)
			case *parser.CaseExpr:
				stmts = append(stmts, stmt)
			case *parser.SubstrFunc:
				stmts = append(stmts, stmt)
			case *parser.TrimFunc:
				stmts = append(stmts, stmt)
			case *parser.LengthFunc:
				stmts = append(stmts, stmt)
			case *parser.PositionFunc:
				stmts = append(stmts, stmt)
			case *parser.RoundFunc:
				stmts = append(stmts, stmt)
			case *parser.ReverseFunc:
				stmts = append(stmts, stmt)
			case *parser.CoalesceFunc:
				stmts = append(stmts, stmt)
			case *parser.CastFunc:
				stmts = append(stmts, stmt)
			case *parser.LowerFunc:
				stmts = append(stmts, stmt)
			case *parser.UpperFunc:
				stmts = append(stmts, stmt)
			case *parser.ProcedureStmt:
				stmts = append(stmts, stmt)
			case *parser.Parameter:
				stmts = append(stmts, stmt)
			case *parser.PrivilegeDefinition:
				stmts = append(stmts, stmt)
			case *parser.BeginStmt:
				stmts = append(stmts, stmt)
			case *parser.CommitStmt:
				stmts = append(stmts, stmt)
			case *parser.RollbackStmt:
				stmts = append(stmts, stmt)
			case *parser.CreateProcedureStmt:
				stmts = append(stmts, stmt)
			case *parser.DropProcedureStmt:
				stmts = append(stmts, stmt)

			default:
				return nil, errors.New("unknown statement type found in WAL")
			}
		}

	}

	return stmts, nil
}
