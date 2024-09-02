package parser

import (
	"fmt"
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
