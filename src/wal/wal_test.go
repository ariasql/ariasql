// Package wal storage
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
	"os"
	"testing"
)

func TestOpenWAL(t *testing.T) {
	defer os.Remove("wal.dat")

	wal, err := OpenWAL("wal.dat", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	defer wal.Close()
}

func TestWAL_Append(t *testing.T) {
	defer os.Remove("wal.dat")
	defer os.Remove("wal.dat.del")

	wal, err := OpenWAL("wal.dat", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	defer wal.Close()

	err = wal.Append(wal.Encode(parser.CreateDatabaseStmt{Name: &parser.Identifier{Value: "test"}}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWAL_Recover(t *testing.T) {
	defer os.Remove("wal.dat")
	defer os.Remove("wal.dat.del")

	wal, err := OpenWAL("wal.dat", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	defer wal.Close()

	err = wal.Append(wal.Encode(&parser.CreateDatabaseStmt{Name: &parser.Identifier{Value: "test"}}))
	if err != nil {
		t.Fatal(err)
	}

	stmts, err := wal.Recover()
	if err != nil {
		t.Fatal(err)
	}

	if len(stmts) != 1 {
		t.Fatal("expected 1 statement")
	}

	if stmts[0].(parser.CreateDatabaseStmt).Name.Value != "test" {
		t.Fatal("expected test")
	}
}