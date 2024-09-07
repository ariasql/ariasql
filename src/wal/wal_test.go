// Package wal storage
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

func TestWAL_RecoverASTs(t *testing.T) {
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

	err = wal.Append(wal.Encode(&parser.UseStmt{DatabaseName: &parser.Identifier{Value: "test"}}))
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Append(wal.Encode(&parser.CreateTableStmt{TableName: &parser.Identifier{Value: "users"}}))
	if err != nil {
		t.Fatal(err)
	}

	err = wal.Append(wal.Encode(&parser.InsertStmt{
		TableName:   &parser.Identifier{Value: "users"},
		ColumnNames: []*parser.Identifier{{Value: "user_id"}, {Value: "users"}},
		Values: [][]*parser.Literal{
			{{Value: 1}, {Value: "frankenstein"}},
			{{Value: 2}, {Value: "frankenstein"}},
			{{Value: 3}, {Value: "drako"}},
		}},
	))
	if err != nil {
		t.Fatal(err)
	}

	asts, err := wal.RecoverASTs()
	if err != nil {
		t.Fatal(err)
	}

	if len(asts) != 4 {
		t.Fatalf("expected 4 ast, got %d", len(asts))
	}

	for _, ast := range asts {
		switch ast.(type) {
		case *parser.CreateDatabaseStmt:
			if ast.(*parser.CreateDatabaseStmt).Name.Value != "test" {
				t.Fatalf("expected test, got %s", ast.(*parser.CreateDatabaseStmt).Name.Value)
			}
		case *parser.UseStmt:
			if ast.(*parser.UseStmt).DatabaseName.Value != "test" {
				t.Fatalf("expected test, got %s", ast.(*parser.UseStmt).DatabaseName.Value)
			}
		case *parser.CreateTableStmt:
			if ast.(*parser.CreateTableStmt).TableName.Value != "users" {
				t.Fatalf("expected users, got %s", ast.(*parser.CreateTableStmt).TableName.Value)
			}
		case *parser.InsertStmt:
			if ast.(*parser.InsertStmt).TableName.Value != "users" {
				t.Fatalf("expected users, got %s", ast.(*parser.InsertStmt).TableName.Value)
			}
		}
	}

}
