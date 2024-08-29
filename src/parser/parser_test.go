// Package parser tests
// AriaSQL parser package tests
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
package parser

import "testing"

func TestNewLexer(t *testing.T) {
	lexer := NewLexer([]byte("CREATE DATABASE test;"))
	if lexer == nil {
		t.Fatal("expected non-nil lexer")
	}

	lexer.tokenize()

	if len(lexer.tokens) != 4 {
		t.Fatalf("expected 4 tokens, got %d", len(lexer.tokens))
	}

}

func TestNewParserCreateDatabase(t *testing.T) {
	lexer := NewLexer([]byte("CREATE DATABASE test;"))
	t.Log("Testing: CREATE DATABASE test;")

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createDBStmt, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}

	if createDBStmt.Name.Value != "test" {
		t.Fatalf("expected test, got %s", createDBStmt.Name.Value)
	}
}

func TestNewParserCreateSchema(t *testing.T) {
	lexer := NewLexer([]byte("CREATE SCHEMA test;"))
	t.Log("Testing: CREATE SCHEMA test;")

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createSchemaStmt, ok := stmt.(*CreateSchemaStmt)
	if !ok {
		t.Fatalf("expected *CreateSchemaStmt, got %T", stmt)
	}

	if createSchemaStmt.Name.Value != "test" {
		t.Fatalf("expected test, got %s", createSchemaStmt.Name.Value)
	}
}

func TestNewParserCreateIndex(t *testing.T) {
	lexer := NewLexer([]byte("CREATE INDEX test_idx ON s1.test (col1, col2);"))
	t.Log("Testing: CREATE INDEX test_idx ON s1.test (col1, col2);")

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected *CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName.Value != "test_idx" {
		t.Fatalf("expected test, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.SchemaName.Value != "s1" {
		t.Fatalf("expected schema, got %s", createIndexStmt.SchemaName.Value)
	}

	if createIndexStmt.TableName.Value != "test" {
		t.Fatalf("expected test, got %s", createIndexStmt.TableName.Value)

	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}

	if createIndexStmt.ColumnNames[1].Value != "col2" {
		t.Fatalf("expected col2, got %s", createIndexStmt.ColumnNames[1].Value)
	}
}

func TestNewParserCreateIndex2(t *testing.T) {
	lexer := NewLexer([]byte("CREATE UNIQUE INDEX test_idx ON s1.test (col1, col2);"))
	t.Log("Testing: CREATE UNIQUE INDEX test_idx ON s1.test (col1, col2);")

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected *CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName.Value != "test_idx" {
		t.Fatalf("expected test, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.SchemaName.Value != "s1" {
		t.Fatalf("expected schema, got %s", createIndexStmt.SchemaName.Value)
	}

	if createIndexStmt.TableName.Value != "test" {
		t.Fatalf("expected test, got %s", createIndexStmt.TableName.Value)

	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}

	if createIndexStmt.ColumnNames[1].Value != "col2" {
		t.Fatalf("expected col2, got %s", createIndexStmt.ColumnNames[1].Value)
	}

	if !createIndexStmt.Unique {
		t.Fatalf("expected unique index")
	}
}

