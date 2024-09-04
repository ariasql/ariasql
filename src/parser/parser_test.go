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

	if insertStmt.Values[0][0].Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %d", insertStmt.Values[0][0].Value)
	}

	if insertStmt.Values[0][1].Value.(string) != "'hello'" {
		t.Fatalf("expected 'hello', got %s", insertStmt.Values[0][1].Value)

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

	sel, err := PrintAST(grantStmt)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(sel)

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

	sel, err := PrintAST(grantStmt)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(sel)

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

	sel, err := PrintAST(grantStmt)
	if err != nil {
		t.Fatal(err)
	}

	log.Println(sel)

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
