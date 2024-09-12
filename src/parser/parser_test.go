// Package parser tests
// AriaSQL parser tests
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

import (
	"ariasql/shared"
	"fmt"
	"log"
	"testing"
)

func TestNewParserCreateDatabase(t *testing.T) {
	statement := []byte(`
	CREATE DATABASE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createDatabaseStmt, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)

	}

	if err != nil {
		t.Fatal(err)

	}

	if createDatabaseStmt.Name.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createDatabaseStmt.Name.Value)
	}

}

func TestNewParserUseDatabase(t *testing.T) {
	statement := []byte(`
	USE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	useDatabaseStmt, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("expected *UseDatabaseStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if useDatabaseStmt.DatabaseName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", useDatabaseStmt.DatabaseName.Value)
	}
}

func TestNewParserCreateTable(t *testing.T) {
	statement := []byte(`
	CREATE TABLE TEST (col1 INT, col2 CHAR(255), deci DECIMAL(10, 2) );
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createTableStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if createTableStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType != "CHAR" {
		t.Fatalf("expected CHAR, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Length != 255 {
		t.Fatalf("expected 255, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Length)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].DataType != "DECIMAL" {
		t.Fatalf("expected DECIMAL, got %s", createTableStmt.TableSchema.ColumnDefinitions["deci"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].Precision != 10 {
		t.Fatalf("expected 10, got %d", createTableStmt.TableSchema.ColumnDefinitions["deci"].Precision)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].Scale != 2 {
		t.Fatalf("expected 2, got %d", createTableStmt.TableSchema.ColumnDefinitions["deci"].Scale)

	}

}

func TestNewParserCreateTable2(t *testing.T) {
	statement := []byte(`
	CREATE TABLE TEST (col1 INT SEQUENCE NOT NULL UNIQUE, col2 CHAR(255) UNIQUE, deci DECIMAL(10, 2) );
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createTableStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if createTableStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].Sequence != true {
		t.Fatalf("expected true, got %v", createTableStmt.TableSchema.ColumnDefinitions["col1"].Sequence)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].NotNull != true {
		t.Fatalf("expected true, got %v", createTableStmt.TableSchema.ColumnDefinitions["col1"].NotNull)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].Unique != true {
		t.Fatalf("expected true, got %v", createTableStmt.TableSchema.ColumnDefinitions["col1"].Unique)

	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType != "CHAR" {
		t.Fatalf("expected CHAR, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Length != 255 {
		t.Fatalf("expected 255, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Length)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Unique != true {
		t.Fatalf("expected true, got %v", createTableStmt.TableSchema.ColumnDefinitions["col2"].Unique)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].DataType != "DECIMAL" {
		t.Fatalf("expected DECIMAL, got %s", createTableStmt.TableSchema.ColumnDefinitions["deci"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].Precision != 10 {
		t.Fatalf("expected 10, got %d", createTableStmt.TableSchema.ColumnDefinitions["deci"].Precision)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["deci"].Scale != 2 {
		t.Fatalf("expected 2, got %d", createTableStmt.TableSchema.ColumnDefinitions["deci"].Scale)

	}

}

func TestNewParserCreateIndex(t *testing.T) {
	statement := []byte(`
	CREATE INDEX idx1 ON TEST (col1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	if err != nil {
		t.Fatal(err)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Value)
	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}

}

func TestNewParserCreateIndexUnique(t *testing.T) {
	statement := []byte(`
	CREATE UNIQUE INDEX idx1 ON TEST (col1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	if err != nil {
		t.Fatal(err)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Value)
	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}

	if createIndexStmt.Unique != true {
		t.Fatalf("expected true, got %v", createIndexStmt.Unique)
	}

}

func TestNewParserCreateIndex2(t *testing.T) {
	// multiple columns

	statement := []byte(`
	CREATE UNIQUE INDEX idx1 ON TEST (col1, col2, col3);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	if err != nil {
		t.Fatal(err)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Value)
	}

	for i, col := range createIndexStmt.ColumnNames {
		if col.Value != fmt.Sprintf("col%d", i+1) {
			t.Fatalf("expected col%d, got %s", i+1, col.Value)
		}
	}

	if createIndexStmt.Unique != true {
		t.Fatalf("expected true, got %v", createIndexStmt.Unique)

	}

}

func TestNewParserCreateIndexUnique2(t *testing.T) {
	// multiple columns

	statement := []byte(`
	CREATE INDEX idx1 ON TEST (col1, col2, col3);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	if err != nil {
		t.Fatal(err)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Value)
	}

	for i, col := range createIndexStmt.ColumnNames {
		if col.Value != fmt.Sprintf("col%d", i+1) {
			t.Fatalf("expected col%d, got %s", i+1, col.Value)
		}
	}

}

func TestNewParserInsert(t *testing.T) {
	statement := []byte(`
	INSERT INTO TEST (col1, col2) VALUES (1, 'hello'), (2, 'world');
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	insertStmt, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if insertStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", insertStmt.TableName.Value)
	}

	if len(insertStmt.ColumnNames) != 2 {
		t.Fatalf("expected 2, got %d", len(insertStmt.ColumnNames))
	}

	for i, col := range insertStmt.ColumnNames {
		if col.Value != fmt.Sprintf("col%d", i+1) {
			t.Fatalf("expected col%d, got %s", i+1, col.Value)
		}

	}

	if len(insertStmt.Values) != 2 {
		t.Fatalf("expected 2, got %d", len(insertStmt.Values))
	}

	if insertStmt.Values[0][0].(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", insertStmt.Values[0][0].(*Literal).Value)
	}

	if insertStmt.Values[0][1].(*Literal).Value.(string) != "'hello'" {
		t.Fatalf("expected 'hello', got %s", insertStmt.Values[0][1].(*Literal).Value)

	}

}

func TestNewParserDropDatabase(t *testing.T) {
	statement := []byte(`
	DROP DATABASE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	dropDatabaseStmt, ok := stmt.(*DropDatabaseStmt)
	if !ok {
		t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if dropDatabaseStmt.Name.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", dropDatabaseStmt.Name.Value)
	}

}

func TestNewParserDropTable(t *testing.T) {
	statement := []byte(`
	DROP TABLE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	dropTableStmt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("expected *DropTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if dropTableStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", dropTableStmt.TableName.Value)
	}

}

func TestNewParserDropIndex(t *testing.T) {
	statement := []byte(`
	DROP INDEX idx1 ON TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	dropIndexStmt, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("expected *DropIndexStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if dropIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", dropIndexStmt.IndexName.Value)
	}

	if dropIndexStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", dropIndexStmt.TableName.Value)
	}

}

func TestNewParserSelect(t *testing.T) {
	statement := []byte(`
	SELECT 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*Literal).Value)
	}

}

func TestNewParserSelect2(t *testing.T) {
	statement := []byte(`
	SELECT 1+1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*Literal).Value)
	}

}

func TestNewParserSelect3(t *testing.T) {
	statement := []byte(`
	SELECT 1+1*2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op != OP_MULT {
		t.Fatalf("expected *, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value)
	}
}

func TestNewParserSelect4(t *testing.T) {
	statement := []byte(`
	SELECT 1+1*(2+23);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op != OP_MULT {
		t.Fatalf("expected *, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(23) {
		t.Fatalf("expected 23, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value)
	}

}

func TestNewParserSelect5(t *testing.T) {
	statement := []byte(`
	SELECT 'hello world';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*Literal).Value.(string) != "'hello world'" {
		t.Fatalf("expected hello world, got %s", selectStmt.SelectList.Expressions[0].Value.(*Literal).Value)
	}

}

func TestNewParserSelect6(t *testing.T) {
	statement := []byte(`
	SELECT SUM(SUM(c));
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "SUM" {
		t.Fatalf("expected SUM, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*AggregateFunc).FuncName != "SUM" {
		t.Fatalf("expected SUM, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "c" {
		t.Fatalf("expected c, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}
}

func TestNewParserSelect7(t *testing.T) {
	statement := []byte(`
	SELECT SUM(SUM(c+1))*22+1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Op != OP_MULT {
		t.Fatalf("expected *, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Op)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).FuncName != "SUM" {
		t.Fatalf("expected SUM, got %s", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).FuncName != "SUM" {
		t.Fatalf("expected SUM, got %s", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value != "c" {
		t.Fatalf("expected c, got %s", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Left.(*AggregateFunc).Args[0].(*AggregateFunc).Args[0].(*BinaryExpression).Right.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(22) {
		t.Fatalf("expected 22, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*BinaryExpression).Right.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*Literal).Value)
	}

}

func TestNewParserSelect8(t *testing.T) {
	statement := []byte(`
	SELECT col, col2, col3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 3 {
		t.Fatalf("expected 3 expressions, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col" {
		t.Fatalf("expected col, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).ColumnName.Value != "col3" {
		t.Fatalf("expected col3, got %s", selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).ColumnName.Value)
	}
}

func TestNewParserSelect9(t *testing.T) {
	statement := []byte(`
	SELECT tbl.col, tbl2.col2, tbl3.col3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 3 {
		t.Fatalf("expected 3 expressions, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).TableName.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col" {
		t.Fatalf("expected col, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).TableName.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).TableName.Value != "tbl3" {
		t.Fatalf("expected tbl3, got %s", selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).ColumnName.Value != "col3" {
		t.Fatalf("expected col3, got %s", selectStmt.SelectList.Expressions[2].Value.(*ColumnSpecification).ColumnName.Value)
	}

}

func TestNewParserSelect10(t *testing.T) {
	statement := []byte(`
	SELECT col FROM tbl1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col" {
		t.Fatalf("expected col, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect11(t *testing.T) {
	statement := []byte(`
	SELECT tbl1.col1, tbl2.col2 FROM tbl1, tbl2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 2 {
		t.Fatalf("expected 2 expression, got %d", len(selectStmt.SelectList.Expressions))

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).TableName.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

}

func TestNewParserSelect12(t *testing.T) {
	statement := []byte(`
	SELECT col1, col2 FROM tbl1 WHERE col1 <> 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_NEQ {
		t.Fatalf("expected !=, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)

	}
}

func TestNewParserSelect13(t *testing.T) {
	statement := []byte(`
	SELECT tbl1.col1, tbl2.col2 FROM tbl1, tbl2 WHERE tbl1.col1+1 <> tbl2.col2+1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_NEQ {
		t.Fatalf("expected !=, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Left.(*ColumnSpecification).TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Left.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*BinaryExpression).Right.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Left.(*ColumnSpecification).TableName.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Left.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Left.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Right.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*BinaryExpression).Right.(*Literal).Value)
	}

}

func TestNewParserSelect14(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 <> 1 AND col2 = 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op != OP_AND {
		t.Fatalf("expected AND, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op != OP_NEQ {
		t.Fatalf("expected <>, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserSelect15(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 <> 1 OR col2 = 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op != OP_OR {
		t.Fatalf("expected AND, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op != OP_NEQ {
		t.Fatalf("expected <>, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserSelect16(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 BETWEEN 1 AND 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Lower.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Lower.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Upper.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*BetweenPredicate).Upper.Value.(*Literal).Value)
	}

}

func TestNewParserSelect17(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 BETWEEN 1 AND 2 AND col2 = 3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op != OP_AND {
		t.Fatalf("expected AND, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Lower.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Lower.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Upper.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*BetweenPredicate).Upper.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(3) {
		t.Fatalf("expected 3, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}
}

func TestNewParserSelect18(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 IN (1, 2, 3);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	for i, v := range selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Values {
		if v.Value.(*Literal).Value.(uint64) != uint64(i+1) {
			t.Fatalf("expected %d, got %d", i+1, v.Value.(*Literal).Value)
		}
	}

}

func TestNewParserSelect19(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 IN (SELECT col2 FROM tbl2);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Values[0].Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Values[0].Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Values[0].Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*InPredicate).Values[0].Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect20(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 LIKE 'a%';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LikePredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LikePredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LikePredicate).Pattern.Value.(*Literal).Value.(string) != "'a%'" {
		t.Fatalf("expected a%%, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LikePredicate).Pattern.Value.(*Literal).Value)
	}

}

func TestNewParserSelect21(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 IS NULL;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null != true {
		t.Fatalf("expected true, got %t", selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null)
	}

}

func TestNewParserSelect22(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 IS NOT NULL;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null == true {
		t.Fatalf("expected false, got %t", selectStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null)
	}

}

func TestNewParserSelect23(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 NOT BETWEEN 1 AND 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Lower.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Lower.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Upper.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*BetweenPredicate).Upper.Value.(*Literal).Value)
	}
}

func TestNewParserSelect24(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 NOT LIKE '%a';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*LikePredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*LikePredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*LikePredicate).Pattern.Value.(*Literal).Value.(string) != "'%a'" {
		t.Fatalf("expected %%a, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*LikePredicate).Pattern.Value.(*Literal).Value)
	}

}

func TestNewParserSelect25(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 NOT IN (1, 2, 3);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[0].Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[0].Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[1].Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[1].Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[2].Value.(*Literal).Value.(uint64) != uint64(3) {
		t.Fatalf("expected 3, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*NotExpr).Expr.(*InPredicate).Values[2].Value.(*Literal).Value)
	}

}

func TestNewParserSelect26(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE col1 = (SELECT col2 FROM tbl2 WHERE col2 = 1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ValueExpression).Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}
}

func TestNewParserSelect27(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 WHERE EXISTS (SELECT col2 FROM tbl2 WHERE tbl2.col2 = tbl1.col1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ExistsPredicate).Expr.Value.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value)
	}
}

func TestNewParserSelect28(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1 GROUP BY col1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.GroupByClause.GroupByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.GroupByClause.GroupByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}
}

func TestNewParserSelect29(t *testing.T) {
	statement := []byte(`
	SELECT COUNT(col1) FROM tbl1 GROUP BY col1 HAVING COUNT(col1) > 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.GroupByClause.GroupByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.GroupByClause.GroupByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.HavingClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}
}

func TestNewParserSelect30(t *testing.T) {
	statement := []byte(`
	SELECT COUNT(col1) FROM tbl1 ORDER BY col1 DESC;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.OrderByClause.Order == ASC {
		t.Fatalf("expected DESC, got %d", selectStmt.TableExpression.OrderByClause.Order)
	}

}

func TestNewParserSelect32(t *testing.T) {
	statement := []byte(`
	SELECT COUNT(col1) FROM tbl1 ORDER BY col1 DESC LIMIT 1 OFFSET 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.OrderByClause.Order == ASC {
		t.Fatalf("expected DESC, got %d", selectStmt.TableExpression.OrderByClause.Order)
	}

	if selectStmt.TableExpression.LimitClause.Count.Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.LimitClause.Count.Value.(uint64))
	}

	if selectStmt.TableExpression.LimitClause.Offset.Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.TableExpression.LimitClause.Offset.Value.(uint64))
	}

}

func TestNewParserSelect31(t *testing.T) {
	statement := []byte(`
	SELECT COUNT(col1) FROM tbl1 ORDER BY col1 DESC LIMIT 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.OrderByClause.OrderByExpressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.OrderByClause.Order == ASC {
		t.Fatalf("expected DESC, got %d", selectStmt.TableExpression.OrderByClause.Order)
	}

	if selectStmt.TableExpression.LimitClause.Count.Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.TableExpression.LimitClause.Count.Value.(uint64))
	}

}

func TestNewParserUpdate(t *testing.T) {
	statement := []byte(`
	UPDATE tbl1 SET col1 = 1 WHERE col2 = 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	updateStmt, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(updateStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if updateStmt.TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", updateStmt.TableName.Value)
	}

	if updateStmt.SetClause[0].Column.Value != "col1" {
		t.Fatalf("expected col1, got %s", updateStmt.SetClause[0].Column.Value)
	}

	if updateStmt.SetClause[0].Value.Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", updateStmt.SetClause[0].Value.Value.(uint64))
	}

	if updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Op)

	}

	if updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", updateStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

}

func TestNewParserDelete(t *testing.T) {
	statement := []byte(`
	DELETE FROM tbl1 WHERE col1 = 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	deleteStmt, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(deleteStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if deleteStmt.TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", deleteStmt.TableName.Value)
	}

	if deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", deleteStmt.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

}

func TestNewParserBegin(t *testing.T) {
	statement := []byte(`
	BEGIN;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	beginStmt, ok := stmt.(*BeginStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(beginStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if beginStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

func TestNewParserRollback(t *testing.T) {
	statement := []byte(`
	ROLLBACK;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	rollbackStmt, ok := stmt.(*RollbackStmt)
	if !ok {
		t.Fatalf("expected *RollbackStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if rollbackStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

func TestNewParserCommit(t *testing.T) {
	statement := []byte(`
	COMMIT;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	commitStmt, ok := stmt.(*CommitStmt)
	if !ok {
		t.Fatalf("expected *CommitStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if commitStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

func TestNewParserCreateUserStmt(t *testing.T) {
	statement := []byte(`
	CREATE USER username IDENTIFIED BY 'password';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createUserStmt, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}

	//sel, err := PrintAST(createUserStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if createUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if createUserStmt.Username.Value != "username" {
		t.Fatalf("expected username, got %s", createUserStmt.Username.Value)
	}

	if createUserStmt.Password.Value != "password" {
		t.Fatalf("expected password, got %s", createUserStmt.Password.Value)
	}

}

func TestNewParserGrantStmt(t *testing.T) {
	statement := []byte(`
	GRANT CONNECT TO username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}

	//sel, err := PrintAST(grantStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if grantStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if grantStmt.PrivilegeDefinition.Actions[0] != shared.PRIV_CONNECT {
		t.Fatalf("expected CONNECT, got %d", grantStmt.PrivilegeDefinition.Actions[0])
	}

	if grantStmt.PrivilegeDefinition.Grantee.Value != "username" {
		t.Fatalf("expected username, got %s", grantStmt.PrivilegeDefinition.Grantee.Value)
	}

}

func TestNewParserGrantStmt2(t *testing.T) {
	statement := []byte(`
	GRANT SELECT ON db1.tbl1 TO username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}

	//sel, err := PrintAST(grantStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if grantStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if grantStmt.PrivilegeDefinition.Actions[0] != shared.PRIV_SELECT {
		t.Fatalf("expected SELECT, got %d", grantStmt.PrivilegeDefinition.Actions[0])
	}

	if grantStmt.PrivilegeDefinition.Grantee.Value != "username" {
		t.Fatalf("expected username, got %s", grantStmt.PrivilegeDefinition.Grantee.Value)
	}

	if grantStmt.PrivilegeDefinition.Object.Value != "db1.tbl1" {
		t.Fatalf("expected db1.tbl1, got %s", grantStmt.PrivilegeDefinition.Object.Value)
	}

}

func TestNewParserGrantStmt4(t *testing.T) {
	statement := []byte(`
	GRANT SELECT, CREATE, DROP ON db1.* TO username;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}

	//sel, err := PrintAST(grantStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if grantStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if grantStmt.PrivilegeDefinition.Actions[0] != shared.PRIV_SELECT {
		t.Fatalf("expected SELECT, got %d", grantStmt.PrivilegeDefinition.Actions[0])
	}

	if grantStmt.PrivilegeDefinition.Actions[1] != shared.PRIV_CREATE {
		t.Fatalf("expected SELECT, got %d", grantStmt.PrivilegeDefinition.Actions[1])
	}

	if grantStmt.PrivilegeDefinition.Actions[2] != shared.PRIV_DROP {
		t.Fatalf("expected SELECT, got %d", grantStmt.PrivilegeDefinition.Actions[2])
	}

	if grantStmt.PrivilegeDefinition.Grantee.Value != "username" {
		t.Fatalf("expected username, got %s", grantStmt.PrivilegeDefinition.Grantee.Value)
	}

	if grantStmt.PrivilegeDefinition.Object.Value != "db1.*" {
		t.Fatalf("expected db1.*, got %s", grantStmt.PrivilegeDefinition.Object.Value)
	}

}

func TestNewParserRevokeStmt(t *testing.T) {
	statement := []byte(`
	REVOKE CREATE, DROP ON db1.* TO username;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	revokeStmt, ok := stmt.(*RevokeStmt)
	if !ok {
		t.Fatalf("expected *RevokeStmt, got %T", stmt)
	}

	//sel, err := PrintAST(revokeStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if revokeStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if revokeStmt.PrivilegeDefinition.Actions[0] != shared.PRIV_CREATE {
		t.Fatalf("expected CREATE, got %d", revokeStmt.PrivilegeDefinition.Actions[0])
	}

	if revokeStmt.PrivilegeDefinition.Actions[1] != shared.PRIV_DROP {
		t.Fatalf("expected DROP, got %d", revokeStmt.PrivilegeDefinition.Actions[1])
	}

	if revokeStmt.PrivilegeDefinition.Revokee.Value != "username" {
		t.Fatalf("expected username, got %s", revokeStmt.PrivilegeDefinition.Revokee.Value)
	}

	if revokeStmt.PrivilegeDefinition.Object.Value != "db1.*" {
		t.Fatalf("expected db1.*, got %s", revokeStmt.PrivilegeDefinition.Object.Value)
	}

}

func TestNewParserDropUserStmt(t *testing.T) {
	statement := []byte(`
	DROP USER username;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	dropUserStmt, ok := stmt.(*DropUserStmt)
	if !ok {
		t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
	}

	if dropUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if dropUserStmt.Username.Value != "username" {
		t.Fatalf("expected username, got %s", dropUserStmt.Username.Value)
	}

}

func TestNewParserShowStmt(t *testing.T) {
	statement := []byte(`
	SHOW DATABASES;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_DATABASES {
		t.Fatalf("expected SHOW DATABASES, got %d", showStmt.ShowType)
	}
}

func TestNewParserShowStmt2(t *testing.T) {
	statement := []byte(`
	SHOW TABLES;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_TABLES {
		t.Fatalf("expected SHOW TABLES, got %d", showStmt.ShowType)
	}
}

func TestNewParserShowStmt3(t *testing.T) {
	statement := []byte(`
	SHOW USERS;
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_USERS {
		t.Fatalf("expected SHOW USERS, got %d", showStmt.ShowType)
	}
}

func TestNewParserAlterUser(t *testing.T) {
	statement := []byte(`
	ALTER USER admin SET PASSWORD 'newpassword';
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	alterUserStmt, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}

	if alterUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	//sel, err := PrintAST(alterUserStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if alterUserStmt.Username.Value != "admin" {
		t.Fatalf("expected admin, got %s", alterUserStmt.Username.Value)
	}

	if alterUserStmt.Value.Value != "newpassword" {
		t.Fatalf("expected newpassword, got %s", alterUserStmt.Value.Value)
	}

	if alterUserStmt.SetType != ALTER_USER_SET_PASSWORD {
		t.Fatalf("expected PASSWORD, got %d", alterUserStmt.SetType)
	}

}

func TestNewParserAlterUser2(t *testing.T) {
	statement := []byte(`
	ALTER USER admin SET USERNAME 'newusername';
`)

	lexer := NewLexer(statement)

	t.Log(string(statement))

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

	alterUserStmt, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}

	if alterUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	//sel, err := PrintAST(alterUserStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if alterUserStmt.Username.Value != "admin" {
		t.Fatalf("expected admin, got %s", alterUserStmt.Username.Value)
	}

	if alterUserStmt.Value.Value != "newusername" {
		t.Fatalf("expected newpassword, got %s", alterUserStmt.Value.Value)
	}

	if alterUserStmt.SetType != ALTER_USER_SET_USERNAME {
		t.Fatalf("expected USERNAME, got %d", alterUserStmt.SetType)
	}

}

func TestNewParserSelect33(t *testing.T) {
	statement := []byte(`
	SELECT * FROM orders2, stores2 WHERE orders2.store_id = stores2.store_id AND stores2.store_name = 'Amazon';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}
	//
	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "orders2" {
		t.Fatalf("expected orders2, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Name.Value != "stores2" {
		t.Fatalf("expected stores2, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value != "orders2" {
		t.Fatalf("expected orders2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value)

	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "store_id" {
		t.Fatalf("expected store_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value != "stores2" {
		t.Fatalf("expected stores2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value)

	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value != "store_id" {
		t.Fatalf("expected store_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Left.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value)

	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value != "stores2" {
		t.Fatalf("expected stores2, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "store_name" {
		t.Fatalf("expected store_name, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value != "'Amazon'" {
		t.Fatalf("expected Amazon, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*LogicalCondition).Right.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserSelect34(t *testing.T) {
	statement := []byte(`
	SELECT * FROM users u, posts p WHERE u.user_id = p.user_id;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "users" {
		t.Fatalf("expected users, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Name.Value != "posts" {
		t.Fatalf("expected posts, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Alias.Value != "u" {
		t.Fatalf("expected users, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Alias.Value != "p" {
		t.Fatalf("expected posts, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value != "u" {
		t.Fatalf("expected u, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "user_id" {
		t.Fatalf("expected user_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value != "p" {
		t.Fatalf("expected p, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value != "user_id" {
		t.Fatalf("expected user_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value)

	}

}

func TestNewParserSelect35(t *testing.T) {
	statement := []byte(`
	SELECT * FROM users as u, posts as p WHERE u.user_id = p.user_id;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "users" {
		t.Fatalf("expected users, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Name.Value != "posts" {
		t.Fatalf("expected posts, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Alias.Value != "u" {
		t.Fatalf("expected users, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[1].Alias.Value != "p" {
		t.Fatalf("expected posts, got %s", selectStmt.TableExpression.FromClause.Tables[1].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value != "u" {
		t.Fatalf("expected u, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "user_id" {
		t.Fatalf("expected user_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value != "p" {
		t.Fatalf("expected p, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).TableName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value != "user_id" {
		t.Fatalf("expected user_id, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*ColumnSpecification).ColumnName.Value)

	}

}

func TestNewParserCreateTable3(t *testing.T) {
	statement := []byte(`
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    EmployeeName CHAR(100) NOT NULL,
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createTableStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	//sel, err := PrintAST(createTableStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if createTableStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if createTableStmt.TableName.Value != "Employees" {
		t.Fatalf("expected Employees, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].Unique != true {
		t.Fatalf("expected true, got %t", createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].Unique)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].NotNull != true {
		t.Fatalf("expected true, got %t", createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].NotNull)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].Sequence != true {
		t.Fatalf("expected false, got %t", createTableStmt.TableSchema.ColumnDefinitions["EmployeeID"].Sequence)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].DataType != "CHAR" {
		t.Fatalf("expected CHAR, got %s", createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].Length != 100 {
		t.Fatalf("expected 100, got %d", createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].Length)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].NotNull != true {
		t.Fatalf("expected true, got %t", createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].NotNull)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].Sequence != false {
		t.Fatalf("expected false, got %t", createTableStmt.TableSchema.ColumnDefinitions["EmployeeName"].Sequence)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].References.TableName != "Departments" {
		t.Fatalf("expected Departments, got %s", createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].References.TableName)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].References.ColumnName != "DepartmentID" {
		t.Fatalf("expected DepartmentID, got %s", createTableStmt.TableSchema.ColumnDefinitions["DepartmentID"].References.ColumnName)
	}

}

func TestNewParserShowGrants(t *testing.T) {
	statement := []byte(`
SHOW GRANTS FOR username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	showGrantsStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	//sel, err := PrintAST(createTableStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if showGrantsStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showGrantsStmt.ShowType != SHOW_GRANTS {
		t.Fatalf("expected SHOW GRANTS, got %d", showGrantsStmt.ShowType)
	}

	if showGrantsStmt.For.Value != "username" {
		t.Fatalf("expected username, got %s", showGrantsStmt.For.Value)
	}

}

func TestNewParserShowIndexes(t *testing.T) {
	statement := []byte(`
SHOW INDEXES FROM tbl_name;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	showIndexesStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	//sel, err := PrintAST(createTableStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if showIndexesStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showIndexesStmt.ShowType != SHOW_INDEXES {
		t.Fatalf("expected SHOW INDEXES, got %d", showIndexesStmt.ShowType)
	}

	if showIndexesStmt.From.Value != "tbl_name" {
		t.Fatalf("expected tbl_name, got %s", showIndexesStmt.From.Value)
	}
}

func TestNewParserUnion(t *testing.T) {
	statement := []byte(`
	SELECT * FROM tbl1 WHERE col1 = 1
	UNION
	SELECT * FROM tbl2 WHERE col2 = 2
	UNION
	SELECT * FROM tbl2 WHERE col3 = 3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	unionStmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)

	}
	//
	//sel, err := PrintAST(unionStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if unionStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if unionStmt.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", unionStmt.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)

	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", unionStmt.(*SelectStmt).Union.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", unionStmt.(*SelectStmt).Union.Union.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col3" {
		t.Fatalf("expected col3, got %s", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(3) {
		t.Fatalf("expected 3, got %d", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

}

func TestNewParserUnionAll(t *testing.T) {
	statement := []byte(`
	SELECT * FROM tbl1 WHERE col1 = 1
	UNION ALL
	SELECT * FROM tbl2 WHERE col2 = 2
	UNION ALL
	SELECT * FROM tbl2 WHERE col3 = 3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	unionStmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(unionStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if unionStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if unionStmt.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", unionStmt.(*SelectStmt).TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).UnionAll != true {
		t.Fatalf("expected true, got %t", unionStmt.(*SelectStmt).UnionAll)
	}

	if unionStmt.(*SelectStmt).Union.UnionAll != true {
		t.Fatalf("expected true, got %t", unionStmt.(*SelectStmt).Union.UnionAll)
	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)

	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", unionStmt.(*SelectStmt).TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", unionStmt.(*SelectStmt).Union.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", unionStmt.(*SelectStmt).Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.FromClause.Tables[0].Name.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", unionStmt.(*SelectStmt).Union.Union.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col3" {
		t.Fatalf("expected col3, got %s", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64) != uint64(3) {
		t.Fatalf("expected 3, got %d", unionStmt.(*SelectStmt).Union.Union.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value.(uint64))
	}

}

func TestNewParserSelect36(t *testing.T) {
	statement := []byte(`
	SELECT 1+1*(2+1) AS RESULT;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op != OP_MULT {
		t.Fatalf("expected *, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value != uint64(2) {
		t.Fatalf("expected 2, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Left.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Op != OP_PLUS {
		t.Fatalf("expected +, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Op)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*BinaryExpression).Right.(*BinaryExpression).Right.(*BinaryExpression).Right.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "RESULT" {
		t.Fatalf("expected RESULT, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

}

func TestNewParserSelect37(t *testing.T) {
	statement := []byte(`
	SELECT x AS xCol, y AS yCol;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	sel, err := PrintAST(selectStmt)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "x" {
		t.Fatalf("expected x, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "xCol" {
		t.Fatalf("expected xCol, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value != "y" {
		t.Fatalf("expected y, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Alias.Value != "yCol" {
		t.Fatalf("expected yCol, got %s", selectStmt.SelectList.Expressions[1].Alias.Value)
	}
}

func TestNewParserSelect38(t *testing.T) {
	statement := []byte(`
	SELECT COUNT(*) AS C;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).FuncName)
	}

	// Check if selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0] is *Wildcard
	if _, ok := selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0].(*Wildcard); !ok {
		t.Fatalf("expected *Wildcard, got %T", selectStmt.SelectList.Expressions[0].Value.(*AggregateFunc).Args[0])
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "C" {
		t.Fatalf("expected C, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

}

func TestNewParserCreateTable4(t *testing.T) {
	statement := []byte(`
	CREATE TABLE TEST (col1 INT, col2 INT DEFAULT 1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createTableStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if createTableStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Default.(*Literal).Value != uint64(1) {
		t.Fatalf("expected 1, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Default.(*Literal).Value)
	}

}

func TestNewParserCreateTable5(t *testing.T) {
	statement := []byte(`
	CREATE TABLE TEST (col1 INT, col2 INT CHECK(col2 > 22));
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	createTableStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if createTableStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].DataType)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Op)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(22) {
		t.Fatalf("expected 22, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Check.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserInsert2(t *testing.T) {
	statement := []byte(`
	INSERT INTO TEST (col1, col2) VALUES (1, GENERATE_UUID), (2, SYS_DATE);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	insertStmt, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)
	}

	if insertStmt.TableName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", insertStmt.TableName.Value)
	}

	if len(insertStmt.ColumnNames) != 2 {
		t.Fatalf("expected 2, got %d", len(insertStmt.ColumnNames))
	}

	for i, col := range insertStmt.ColumnNames {
		if col.Value != fmt.Sprintf("col%d", i+1) {
			t.Fatalf("expected col%d, got %s", i+1, col.Value)
		}

	}

	if len(insertStmt.Values) != 2 {
		t.Fatalf("expected 2, got %d", len(insertStmt.Values))
	}

	if insertStmt.Values[0][0].(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", insertStmt.Values[0][0].(*Literal).Value)
	}

	if insertStmt.Values[0][1].(*Literal).Value.(string) != "GENERATE_UUID" {
		t.Fatalf("expected 'hello', got %s", insertStmt.Values[0][1].(*Literal).Value)

	}

	if insertStmt.Values[1][0].(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %d", insertStmt.Values[1][0].(*Literal).Value)
	}

	if insertStmt.Values[1][1].(*shared.SysDate) == nil {
		t.Fatalf("expected non-nil SysDate, got nil")
	}

}

func TestNewParserSelect39(t *testing.T) {
	statement := []byte(`
	 SELECT UPPER('hello') AS upper_test;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*UpperFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'hello'" {
		t.Fatalf("expected hello, got %s", selectStmt.SelectList.Expressions[0].Value.(*UpperFunc).Arg.(*ValueExpression).Value.(*Literal).Value)
	}

}

func TestNewParserSelect40(t *testing.T) {
	statement := []byte(`
	 SELECT UPPER(col1) FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*UpperFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*UpperFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect41(t *testing.T) {
	statement := []byte(`
	 SELECT LOWER('hello') AS lower_test;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*LowerFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'hello'" {
		t.Fatalf("expected hello, got %s", selectStmt.SelectList.Expressions[0].Value.(*LowerFunc).Arg.(*ValueExpression).Value.(*Literal).Value)
	}

}

func TestNewParserSelect42(t *testing.T) {
	statement := []byte(`
	 SELECT LOWER(col1) FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*LowerFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*LowerFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect43(t *testing.T) {
	statement := []byte(`
	 SELECT CAST(col1 AS INT) FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)
	//
	//if selectStmt.SelectList == nil {
	//	t.Fatal("expected non-nil SelectList")
	//}

	if selectStmt.SelectList.Expressions[0].Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CastFunc).DataType.Value != "INT" {
		t.Fatalf("expected INT, got %s", selectStmt.SelectList.Expressions[0].Value.(*CastFunc).DataType)
	}

}

func TestNewParserSelect44(t *testing.T) {
	statement := []byte(`
	 SELECT CAST(col1 AS INT) AS cast_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)
	//
	//if selectStmt.SelectList == nil {
	//	t.Fatal("expected non-nil SelectList")
	//}

	if selectStmt.SelectList.Expressions[0].Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CastFunc).DataType.Value != "INT" {
		t.Fatalf("expected INT, got %s", selectStmt.SelectList.Expressions[0].Value.(*CastFunc).DataType)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "cast_test" {
		t.Fatalf("expected cast_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

}

func TestNewParserSelect45(t *testing.T) {
	statement := []byte(`
	 SELECT COALESCE(col1,col2, 'some value') FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)
	//
	//if selectStmt.SelectList == nil {
	//	t.Fatal("expected non-nil SelectList")
	//}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[1].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[1].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Value.(*ValueExpression).Value.(*Literal).Value != "'some value'" {
		t.Fatalf("expected some value, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Value.(*ValueExpression).Value.(*Literal).Value)
	}

}

func TestNewParserSelect46(t *testing.T) {
	statement := []byte(`
	 SELECT COALESCE(col1,col2, 'some value') AS coal_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)
	//
	//if selectStmt.SelectList == nil {
	//	t.Fatal("expected non-nil SelectList")
	//}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[1].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Args[1].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Value.(*ValueExpression).Value.(*Literal).Value != "'some value'" {
		t.Fatalf("expected some value, got %s", selectStmt.SelectList.Expressions[0].Value.(*CoalesceFunc).Value.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "coal_test" {
		t.Fatalf("expected coal_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

}

func TestNewParserSelect47(t *testing.T) {
	statement := []byte(`
	 SELECT REVERSE(col1) FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ReverseFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ReverseFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

}

func TestNewParserSelect48(t *testing.T) {
	statement := []byte(`
	 SELECT REVERSE('hello world') AS rev_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ReverseFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'hello world'" {
		t.Fatalf("expected hello world, got %s", selectStmt.SelectList.Expressions[0].Value.(*ReverseFunc).Arg.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "rev_test" {
		t.Fatalf("expected rev_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

}

func TestNewParserSelect49(t *testing.T) {
	statement := []byte(`
	 SELECT ROUND(col1) FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*RoundFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*RoundFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect50(t *testing.T) {
	statement := []byte(`
	 SELECT ROUND(1.88) AS r_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*RoundFunc).Arg.(*ValueExpression).Value.(*Literal).Value != 1.88 {
		t.Fatalf("expected 1.88, got %f", selectStmt.SelectList.Expressions[0].Value.(*RoundFunc).Arg.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "r_test" {
		t.Fatalf("expected r_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect51(t *testing.T) {
	statement := []byte(`
	 SELECT POSITION(col1 IN 'hello') FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")

	}

	if selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).In.(*ValueExpression).Value.(*Literal).Value != "'hello'" {
		t.Fatalf("expected hello, got %s", selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).In.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}
}

func TestNewParserSelect52(t *testing.T) {
	statement := []byte(`
	 SELECT POSITION(col1 IN col2) AS pos_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")

	}

	if selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).In.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[0].Value.(*PositionFunc).In.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "pos_test" {
		t.Fatalf("expected pos_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}
}

func TestNewParserSelect53(t *testing.T) {
	statement := []byte(`
	 SELECT LENGTH(col1) AS len_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*LengthFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*LengthFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)

	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "len_test" {
		t.Fatalf("expected len_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect54(t *testing.T) {
	statement := []byte(`
	 SELECT LENGTH('hello world') AS len_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*LengthFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'hello world'" {
		t.Fatalf("expected hello world, got %s", selectStmt.SelectList.Expressions[0].Value.(*LengthFunc).Arg.(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "len_test" {
		t.Fatalf("expected len_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect55(t *testing.T) {
	statement := []byte(`
	 SELECT TRIM('  hello world  ') AS tr_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*TrimFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'  hello world  '" {
		t.Fatalf("expected '  hello world  ', got %s", selectStmt.SelectList.Expressions[0].Value.(*TrimFunc).Arg.(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "tr_test" {
		t.Fatalf("expected tr_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect56(t *testing.T) {
	statement := []byte(`
	 SELECT TRIM(col1), col2 FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*TrimFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*TrimFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)

	}

	if selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[1].Value.(*ColumnSpecification).ColumnName.Value)

	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect57(t *testing.T) {
	statement := []byte(`
	 SELECT CONCAT('hello',' ','world') AS con_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*Literal).Value != "'hello'" {
		t.Fatalf("expected 'hello', got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value != "' '" {
		t.Fatalf("expected ' ', got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[2].(*ValueExpression).Value.(*Literal).Value != "'world'" {
		t.Fatalf("expected 'world', got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[2].(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "con_test" {
		t.Fatalf("expected con_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect58(t *testing.T) {
	statement := []byte(`
	 SELECT CONCAT(col1,' ',col2) AS con_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value != "' '" {
		t.Fatalf("expected ' ', got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[2].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.SelectList.Expressions[0].Value.(*ConcatFunc).Args[2].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Alias.Value != "con_test" {
		t.Fatalf("expected con_test, got %s", selectStmt.SelectList.Expressions[0].Alias.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

}

func TestNewParserSelect59(t *testing.T) {
	statement := []byte(`
	 SELECT SUBSTRING('hello world', 1, 5) AS sub_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Arg.(*ValueExpression).Value.(*Literal).Value != "'hello world'" {
		t.Fatalf("expected 'hello world', got %s", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Arg.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).StartPos.Value != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).StartPos.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Length.Value != uint64(5) {
		t.Fatalf("expected 5, got %d", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Length.Value)
	}

}

func TestNewParserSelect60(t *testing.T) {
	statement := []byte(`
	 SELECT SUBSTRING(col1, 1, 5) AS sub_test FROM tbl;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Arg.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).StartPos.Value != uint64(1) {
		t.Fatalf("expected 1, got %d", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).StartPos.Value)
	}

	if selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Length.Value != uint64(5) {
		t.Fatalf("expected 5, got %d", selectStmt.SelectList.Expressions[0].Value.(*SubstrFunc).Length.Value)
	}

}

func TestNewParserSelect61(t *testing.T) {
	statement := []byte(`
	 SELECT * FROM tbl WHERE col1 = SYS_DATE;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause == nil {
		t.Fatal("expected non-nil WhereClause")
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	// check if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value is *shared.SysDate
	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*shared.SysDate) == nil {
		t.Fatalf("expected *SysDate, got %T", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value)
	}

	// Check if correct type
	if _, ok := selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*shared.SysDate); !ok {
		t.Fatalf("expected *SysDate, got %T", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value)
	}
}

func TestNewParserSelect62(t *testing.T) {
	statement := []byte(`
	 SELECT * FROM tbl WHERE CAST(col1 AS INT) = 22;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause == nil {
		t.Fatal("expected non-nil WhereClause")
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CastFunc).Expr.(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CastFunc).DataType.Value != "INT" {
		t.Fatalf("expected INT, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CastFunc).DataType.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(22) {
		t.Fatalf("expected 22, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserSelect63(t *testing.T) {
	statement := []byte(`
	 SELECT * FROM tbl WHERE CONCAT(col1, ' padula') = 'alex padula';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl" {
		t.Fatalf("expected tbl, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause == nil {
		t.Fatal("expected non-nil WhereClause")
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ConcatFunc).Args[0].(*ValueExpression).Value.(*ColumnSpecification).ColumnName.Value)

	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value != "' padula'" {
		t.Fatalf("expected ' padula', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*ConcatFunc).Args[1].(*ValueExpression).Value.(*Literal).Value)

	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value != "'alex padula'" {
		t.Fatalf("expected 'alex padula', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}
}

func TestNewParserSelect64(t *testing.T) {
	statement := []byte(`
	SELECT
		employee_name,
		CASE
			WHEN salary > 50000 THEN 'High'
			WHEN salary BETWEEN 30000 AND 50000 THEN 'Medium'
			ELSE 'Low'
		END AS salary_category
	FROM employees;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "employee_name" {
		t.Fatalf("expected employee_name, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "salary" {
		t.Fatalf("expected salary, got %s", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(50000) {
		t.Fatalf("expected 50000, got %d", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value != "'High'" {
		t.Fatalf("expected 'High', got %s", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "salary" {
		t.Fatalf("expected salary, got %s", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Lower.Value.(*Literal).Value != uint64(30000) {
		t.Fatalf("expected 30000, got %d", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Lower.Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Upper.Value.(*Literal).Value != uint64(50000) {
		t.Fatalf("expected 50000, got %d", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Condition.(*BetweenPredicate).Upper.Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Result.(*ValueExpression).Value.(*Literal).Value != "'Medium'" {
		t.Fatalf("expected 'Medium', got %s", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).WhenClauses[1].Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value != "'Low'" {
		t.Fatalf("expected 'Low', got %s", selectStmt.SelectList.Expressions[1].Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.SelectList.Expressions[1].Alias.Value != "salary_category" {
		t.Fatalf("expected salary_category, got %s", selectStmt.SelectList.Expressions[1].Alias.Value)

	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "employees" {
		t.Fatalf("expected employees, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}
}

func TestNewParserSelect65(t *testing.T) {
	statement := []byte(`
	SELECT *
	FROM employees
	WHERE
		CASE
			WHEN department = 'Sales' THEN
				CASE
					WHEN salary > 40000 THEN TRUE
					ELSE FALSE
				END
			ELSE
				CASE
					WHEN salary > 30000 THEN TRUE
					ELSE FALSE
				END
		END;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "employees" {
		t.Fatalf("expected employees, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause == nil {
		t.Fatal("expected non-nil WhereClause")
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "department" {
		t.Fatalf("expected department, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != "'Sales'" {
		t.Fatalf("expected 'Sales', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "salary" {
		t.Fatalf("expected salary, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(40000) {
		t.Fatalf("expected 40000, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value != true {
		t.Fatalf("expected TRUE, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value != false {
		t.Fatalf("expected FALSE, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "salary" {
		t.Fatalf("expected salary, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(30000) {
		t.Fatalf("expected 30000, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value != true {
		t.Fatalf("expected TRUE, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value)
	}
}

func TestNewParserSelect66(t *testing.T) {
	statement := []byte(`
	SELECT username
	FROM users
	WHERE CASE 
		WHEN money > 30 THEN 'rich class'
		WHEN money < 30 THEN 'poor class'
		ELSE 'middle class'
		END = 'poor class'
	;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(selectStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "username" {
		t.Fatalf("expected username, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "users" {
		t.Fatalf("expected users, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if selectStmt.TableExpression.WhereClause == nil {
		t.Fatal("expected non-nil WhereClause")
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op != OP_EQ {
		t.Fatalf("expected =, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op != OP_GT {
		t.Fatalf("expected >, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "money" {
		t.Fatalf("expected money, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(30) {
		t.Fatalf("expected 30, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value != "'rich class'" {
		t.Fatalf("expected 'rich class', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[0].Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Op != OP_LT {
		t.Fatalf("expected <, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Op)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "money" {
		t.Fatalf("expected money, got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value != uint64(30) {
		t.Fatalf("expected 30, got %d", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Condition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Result.(*ValueExpression).Value.(*Literal).Value != "'poor class'" {
		t.Fatalf("expected 'poor class', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).WhenClauses[1].Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value != "'middle class'" {
		t.Fatalf("expected 'middle class', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Left.Value.(*CaseExpr).ElseClause.(*ElseClause).Result.(*ValueExpression).Value.(*Literal).Value)
	}

	if selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value != "'poor class'" {
		t.Fatalf("expected 'poor class', got %s", selectStmt.TableExpression.WhereClause.SearchCondition.(*ComparisonPredicate).Right.Value.(*Literal).Value)
	}

}

func TestNewParserDeclare(t *testing.T) {
	statement := []byte(`
	DECLARE @ProductID INT;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	declareStmt, ok := stmt.(*DeclareStmt)
	if !ok {
		t.Fatalf("expected *DeclareStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(declareStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if declareStmt.CursorVariableName.Value != "@ProductID" {
		t.Fatalf("expected @ProductID, got %s", declareStmt.CursorVariableName.Value)
	}

	if declareStmt.CursorVariableDataType.Value != "INT" {
		t.Fatalf("expected INT, got %s", declareStmt.CursorVariableDataType.Value)
	}
}

func TestNewParserDeclare2(t *testing.T) {
	statement := []byte(`
	DECLARE product_cursor CURSOR FOR SELECT ProductID FROM Products WHERE DiscontinuedDate IS NULL; 
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

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

	declareStmt, ok := stmt.(*DeclareStmt)
	if !ok {
		t.Fatalf("expected *DeclareStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(declareStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if declareStmt.CursorName.Value != "product_cursor" {
		t.Fatalf("expected product_cursor, got %s", declareStmt.CursorName.Value)
	}

	if declareStmt.CursorStmt == nil {
		t.Fatal("expected non-nil CursorStmt")
	}

	if declareStmt.CursorStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "ProductID" {
		t.Fatalf("expected ProductID, got %s", declareStmt.CursorStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if declareStmt.CursorStmt.TableExpression.FromClause.Tables[0].Name.Value != "Products" {
		t.Fatalf("expected Products, got %s", declareStmt.CursorStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	if declareStmt.CursorStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value != "DiscontinuedDate" {
		t.Fatalf("expected DiscontinuedDate, got %s", declareStmt.CursorStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Left.Value.(*ColumnSpecification).ColumnName.Value)
	}

	if !declareStmt.CursorStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null {
		t.Fatalf("expected true, got %v", declareStmt.CursorStmt.TableExpression.WhereClause.SearchCondition.(*IsPredicate).Null)
	}
}

func TestNewParserOpen(t *testing.T) {
	statement := []byte(`
	OPEN product_cursor;
`)

	lexer := NewLexer(statement)

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

	openStmt, ok := stmt.(*OpenStmt)
	if !ok {
		t.Fatalf("expected *OpenStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(openStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if openStmt.CursorName.Value != "product_cursor" {
		t.Fatalf("expected product_cursor, got %s", openStmt.CursorName.Value)
	}

}

func TestNewParserClose(t *testing.T) {
	statement := []byte(`
	CLOSE product_cursor;
`)

	lexer := NewLexer(statement)

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

	closeStmt, ok := stmt.(*CloseStmt)
	if !ok {
		t.Fatalf("expected *OpenStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(openStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if closeStmt.CursorName.Value != "product_cursor" {
		t.Fatalf("expected product_cursor, got %s", closeStmt.CursorName.Value)
	}

}

func TestNewParserDeallocate(t *testing.T) {
	statement := []byte(`
	DEALLOCATE @ProductID;
`)

	lexer := NewLexer(statement)

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

	dealloStmt, ok := stmt.(*DeallocateStmt)
	if !ok {
		t.Fatalf("expected *OpenStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(openStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if dealloStmt.CursorVariableName.Value != "@ProductID" {
		t.Fatalf("expected @ProductID, got %s", dealloStmt.CursorVariableName.Value)
	}

}

func TestNewParserDeallocate2(t *testing.T) {
	statement := []byte(`
	DEALLOCATE product_cursor;
`)

	lexer := NewLexer(statement)

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

	dealloStmt, ok := stmt.(*DeallocateStmt)
	if !ok {
		t.Fatalf("expected *OpenStmt, got %T", stmt)
	}

	if err != nil {
		t.Fatal(err)

	}

	//sel, err := PrintAST(openStmt)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//log.Println(sel)

	if dealloStmt.CursorName.Value != "product_cursor" {
		t.Fatalf("expected product_cursor, got %s", dealloStmt.CursorName.Value)
	}

}
