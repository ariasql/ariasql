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

import (
	"ariasql/catalog"
	"log"
	"testing"
)

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

func TestNewParserUse(t *testing.T) {
	lexer := NewLexer([]byte("USE test;"))
	t.Log("Testing: USE test;")

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

	useStmt, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("expected *UseStmt, got %T", stmt)
	}

	if useStmt.DatabaseName.Value != "test" {
		t.Fatalf("expected test, got %s", useStmt.DatabaseName.Value)
	}
}

func TestNewParserInsert(t *testing.T) {
	lexer := NewLexer([]byte("INSERT INTO s1.test (col1, col2) VALUES (1, 'hello'), (2, 'world');"))
	t.Log("Testing: INSERT INTO s1.test (col1, col2) VALUES (1, 'hello'), (2, 'world');")

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

	if insertStmt.SchemaName.Value != "s1" {
		t.Fatalf("expected s1, got %s", insertStmt.SchemaName.Value)

	}

	if insertStmt.TableName.Value != "test" {
		t.Fatalf("expected test, got %s", insertStmt.TableName.Value)
	}

	if insertStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", insertStmt.ColumnNames[0].Value)
	}

	if insertStmt.ColumnNames[1].Value != "col2" {
		t.Fatalf("expected col2, got %s", insertStmt.ColumnNames[1].Value)
	}

	if insertStmt.Values[0][0].Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %v", insertStmt.Values[0][0].Value)
	}

	if insertStmt.Values[0][1].Value != "'hello'" {
		t.Fatalf("expected hello, got %v", insertStmt.Values[0][1].Value)
	}

	if insertStmt.Values[1][0].Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %v", insertStmt.Values[1][0].Value)
	}

	if insertStmt.Values[1][1].Value != "'world'" {
		t.Fatalf("expected world, got %v", insertStmt.Values[1][1].Value)
	}
}

func TestNewParserCreateTable(t *testing.T) {

	lexer := NewLexer([]byte(`CREATE TABLE s1.test (
    			col1 INT,
    			col2 CHAR(5),
    			col3 TEXT,
    			col4 DECIMAL(10, 2),
    			col5 BOOLEAN,
    			col6 UUID,
    			col7 BIGINT
    );`))
	t.Log(`Test: CREATE TABLE s1.test (
    			col1 INT,
    			col2 CHAR(5),
    			col3 TEXT,
    			col4 DECIMAL(10, 2),
    			col5 BOOLEAN,
    			col6 UUID,
    			col7 BIGINT
    );`)

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

	if createTableStmt.SchemaName.Value != "s1" {
		t.Fatalf("expected s1, got %s", createTableStmt.SchemaName.Value)
	}

	if createTableStmt.TableName.Value != "test" {
		t.Fatalf("expected test, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].Datatype != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Datatype != "CHAR" {
		t.Fatalf("expected CHAR, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Length != 5 {
		t.Fatalf("expected 5, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Length)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col3"].Datatype != "TEXT" {
		t.Fatalf("expected TEXT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col3"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Datatype != "DECIMAL" {
		t.Fatalf("expected DECIMAL, got %s", createTableStmt.TableSchema.ColumnDefinitions["col4"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Precision != 10 {
		t.Fatalf("expected 10, got %d", createTableStmt.TableSchema.ColumnDefinitions["col4"].Precision)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Scale != 2 {
		t.Fatalf("expected 2, got %d", createTableStmt.TableSchema.ColumnDefinitions["col4"].Scale)

	}

	if createTableStmt.TableSchema.ColumnDefinitions["col5"].Datatype != "BOOLEAN" {
		t.Fatalf("expected BOOLEAN, got %s", createTableStmt.TableSchema.ColumnDefinitions["col5"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col6"].Datatype != "UUID" {
		t.Fatalf("expected UUID, got %s", createTableStmt.TableSchema.ColumnDefinitions["col6"].Datatype)

	}

}

func TestNewParserCreateTable2(t *testing.T) {
	lexer := NewLexer([]byte(`CREATE TABLE s1.test (
				col1 INT SEQUENCE PRIMARY KEY,
				col2 CHAR(5) UNIQUE DEFAULT 'hello',
				col3 TEXT NOT NULL,
				col4 DECIMAL(10, 2),
				col5 BOOLEAN DEFAULT TRUE,
				col6 UUID,
				col7 BIGINT FOREIGN KEY REFERENCES s1.test2(colU) ON DELETE CASCADE ON UPDATE CASCADE
	);`))
	t.Log(`Test: CREATE TABLE s1.test (
				col1 INT SEQUENCE PRIMARY KEY,
				col2 CHAR(5) UNIQUE DEFAULT 'hello',
				col3 TEXT NOT NULL,
				col4 DECIMAL(10, 2),
				col5 BOOLEAN DEFAULT TRUE,
				col6 UUID,
				col7 BIGINT FOREIGN KEY REFERENCES s1.test2(colU) ON DELETE CASCADE ON UPDATE CASCADE
	);`)

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

	if createTableStmt.SchemaName.Value != "s1" {
		t.Fatalf("expected s1, got %s", createTableStmt.SchemaName.Value)
	}

	if createTableStmt.TableName.Value != "test" {
		t.Fatalf("expected test, got %s", createTableStmt.TableName.Value)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col1"].Datatype != "INT" {
		t.Fatalf("expected INT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col1"].Datatype)
	}

	if !createTableStmt.TableSchema.ColumnDefinitions["col1"].Sequence {
		t.Fatalf("expected sequence")
	}

	if !createTableStmt.TableSchema.ColumnDefinitions["col1"].PrimaryKey {
		t.Fatalf("expected primary key")
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Datatype != "CHAR" {
		t.Fatalf("expected CHAR, got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Length != 5 {
		t.Fatalf("expected 5, got %d", createTableStmt.TableSchema.ColumnDefinitions["col2"].Length)
	}

	if !createTableStmt.TableSchema.ColumnDefinitions["col2"].Unique {
		t.Fatalf("expected unique")
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col2"].Default != "'hello'" {
		t.Fatalf("expected 'hello', got %s", createTableStmt.TableSchema.ColumnDefinitions["col2"].Default)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col3"].Datatype != "TEXT" {
		t.Fatalf("expected TEXT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col3"].Datatype)
	}

	if !createTableStmt.TableSchema.ColumnDefinitions["col3"].NotNull {
		t.Fatalf("expected not null")
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Datatype != "DECIMAL" {
		t.Fatalf("expected DECIMAL, got %s", createTableStmt.TableSchema.ColumnDefinitions["col4"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Precision != 10 {
		t.Fatalf("expected 10, got %d", createTableStmt.TableSchema.ColumnDefinitions["col4"].Precision)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col4"].Scale != 2 {
		t.Fatalf("expected 2, got %d", createTableStmt.TableSchema.ColumnDefinitions["col4"].Scale)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col5"].Datatype != "BOOLEAN" {
		t.Fatalf("expected BOOLEAN, got %s", createTableStmt.TableSchema.ColumnDefinitions["col5"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col5"].Default != true {
		t.Fatalf("expected TRUE, got %s", createTableStmt.TableSchema.ColumnDefinitions["col5"].Default)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col6"].Datatype != "UUID" {
		t.Fatalf("expected UUID, got %s", createTableStmt.TableSchema.ColumnDefinitions["col6"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].Datatype != "BIGINT" {
		t.Fatalf("expected BIGINT, got %s", createTableStmt.TableSchema.ColumnDefinitions["col7"].Datatype)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].IsForeign == false {
		t.Fatalf("expected foreign key")
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignTable != "test2" {
		t.Fatalf("expected test2, got %s", createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignTable)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignColumn != "colU" {
		t.Fatalf("expected colU, got %s", createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignColumn)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignSchema != "s1" {
		t.Fatalf("expected s1, got %s", createTableStmt.TableSchema.ColumnDefinitions["col7"].ForeignSchema)
	}

	if createTableStmt.TableSchema.ColumnDefinitions["col7"].OnDelete != catalog.CascadeActionCascade {
		t.Fatalf("expected CASCADE, got %d", createTableStmt.TableSchema.ColumnDefinitions["col7"].OnDelete)
	}
}

func TestNewParserSelect(t *testing.T) {
	lexer := NewLexer([]byte("SELECT s1.t1.col1 AS banana;"))
	t.Log("Testing: SELECT s1.t1.col1 AS banana;")

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

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).SchemaName.Value != "s1" {
		t.Fatalf("expected s1, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).SchemaName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).TableName.Value != "t1" {
		t.Fatalf("expected t1, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).TableName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).Alias.Value != "banana" {
		t.Fatalf("expected banana, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).Alias.Value)
	}

}

func TestNewParserSelect2(t *testing.T) {
	lexer := NewLexer([]byte("SELECT s1.t1.col1+1*(21+1) AS banana;"))
	t.Log("Testing: SELECT s1.t1.col1 AS banana;")

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

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value != "banana" {
		t.Fatalf("expected banana, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op != "+" {
		t.Fatalf("expected +, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*ColumnSpec).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Op != "*" {
		t.Fatalf("expected *, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Op != "+" {
		t.Fatalf("expected +, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value.(uint64) != uint64(21) {
		t.Fatalf("expected 21, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Right.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*BinaryExpr).Right.(*Literal).Value)
	}

}

func TestNewParserSelect3(t *testing.T) {
	lexer := NewLexer([]byte("SELECT 11+2 AS res;"))
	t.Log("Testing: SELECT 11+2 AS res;")

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

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value != "res" {
		t.Fatalf("expected res, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op != "+" {
		t.Fatalf("expected +, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*Literal).Value.(uint64) != uint64(11) {
		t.Fatalf("expected 11, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*Literal).Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*Literal).Value)
	}

}

func TestNewParserSelect4(t *testing.T) {
	lexer := NewLexer([]byte("SELECT DISTINCT 11+2*COUNT(x.x.x+5) AS res;")) // just for parser tests
	t.Log("Testing: SELECT DISTINCT 11+2*COUNT(x.x.x+5) AS res;")

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

	if selectStmt.Distinct == false {
		t.Fatalf("expected distinct")
	}

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value != "res" {
		t.Fatalf("expected res, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Alias.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op != "+" {
		t.Fatalf("expected +, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*Literal).Value.(uint64) != uint64(11) {
		t.Fatalf("expected 11, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Left.(*Literal).Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Op != "*" {
		t.Fatalf("expected *, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value.(uint64) != uint64(2) {
		t.Fatalf("expected 2, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Left.(*Literal).Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).FuncName != "COUNT" {
		t.Fatalf("expected COUNT, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).FuncName)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Left.(*ColumnSpec).ColumnName.Value != "x" {
		t.Fatalf("expected x, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Left.(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Op != "+" {
		t.Fatalf("expected +, got %s", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Op)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Right.(*Literal).Value.(uint64) != uint64(5) {
		t.Fatalf("expected 5, got %v", selectStmt.ColumnSet.Exprs[0].(*ValueExpr).Value.(*BinaryExpr).Right.(*BinaryExpr).Right.(*AggFunc).Args[0].(*BinaryExpr).Right.(*Literal).Value)
	}
}

func TestNewParserSelect6(t *testing.T) {
	lexer := NewLexer([]byte("SELECT DISTINCT a.col1, b.col2 FROM s1.tbl1 AS a, s2.tbl2 as b;")) // just for parser tests
	t.Log("Testing: SELECT DISTINCT a.col1, b.col2 FROM s1.tbl1 AS a, s2.tbl2 as b;")

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

	if selectStmt.Distinct == false {
		t.Fatalf("expected distinct")
	}

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).TableName.Value != "a" {
		t.Fatalf("expected a, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).TableName.Value)
	}

	if selectStmt.ColumnSet.Exprs[1].(*ColumnSpec).ColumnName.Value != "col2" {
		t.Fatalf("expected col2, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.ColumnSet.Exprs[1].(*ColumnSpec).TableName.Value != "b" {
		t.Fatalf("expected b, got %s", selectStmt.ColumnSet.Exprs[0].(*ColumnSpec).TableName.Value)
	}

	if selectStmt.From == nil {
		t.Fatalf("expected non-nil from clause")
	}

	if selectStmt.From.Tables[0].SchemaName.Value != "s1" {
		t.Fatalf("expected s1, got %s", selectStmt.From.Tables[0].SchemaName.Value)
	}

	if selectStmt.From.Tables[0].TableName.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.From.Tables[0].TableName.Value)
	}

	if selectStmt.From.Tables[0].Alias.Value != "a" {
		t.Fatalf("expected a, got %s", selectStmt.From.Tables[0].Alias.Value)
	}

	if selectStmt.From.Tables[1].SchemaName.Value != "s2" {
		t.Fatalf("expected s2, got %s", selectStmt.From.Tables[1].SchemaName.Value)
	}

	if selectStmt.From.Tables[1].TableName.Value != "tbl2" {
		t.Fatalf("expected tbl2, got %s", selectStmt.From.Tables[1].TableName.Value)
	}

	if selectStmt.From.Tables[1].Alias.Value != "b" {
		t.Fatalf("expected b, got %s", selectStmt.From.Tables[1].Alias.Value)
	}

}

func TestNewParserSelect7(t *testing.T) {
	lexer := NewLexer([]byte("SELECT * FROM x.t as y WHERE y.c1 = 55;")) // just for parser tests
	t.Log("Testing: SELECT * FROM x.t as y WHERE y.c1 = 55;")

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

	if selectStmt.ColumnSet == nil {
		t.Fatalf("expected non-nil column set")
	}

	if selectStmt.From == nil {
		t.Fatalf("expected non-nil from clause")
	}

	if selectStmt.Where == nil {
		t.Fatalf("expected non-nil where clause")
	}

	if selectStmt.Where.Cond.(*ComparisonPredicate).LeftExpr.(*ColumnSpec).TableName.Value != "y" {
		t.Fatalf("expected y, got %s", selectStmt.Where.Cond.(*ComparisonPredicate).LeftExpr.(*ColumnSpec).TableName.Value)
	}

	if selectStmt.Where.Cond.(*ComparisonPredicate).LeftExpr.(*ColumnSpec).ColumnName.Value != "c1" {
		t.Fatalf("expected c1, got %s", selectStmt.Where.Cond.(*ComparisonPredicate).LeftExpr.(*ColumnSpec).ColumnName.Value)
	}

	if selectStmt.Where.Cond.(*ComparisonPredicate).RightExpr.(*ValueExpr).Value.(uint64) != uint64(55) {
		t.Fatalf("expected 55, got %v", selectStmt.Where.Cond.(*ComparisonPredicate).RightExpr.(*ValueExpr).Value)
	}

	if selectStmt.Where.Cond.(*ComparisonPredicate).Operator != Eq {
		t.Fatalf("expected =, got %d", selectStmt.Where.Cond.(*ComparisonPredicate).Operator)
	}
}
