package parser

import (
	"testing"
)

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
