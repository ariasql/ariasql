package parser

import (
	"ariasql/catalog"
	"encoding/json"
)

// Node represents an AST node
type Node interface{}

// Statement represents a SQL statement
type Statement interface {
	Node // All statements are nodes
}

// Identifier represents an identifier, like a table or column name
type Identifier struct {
	Value string
}

// Literal represents a literal value, like a number or string
type Literal struct {
	Value interface{}
}

// CreateDatabaseStmt represents a CREATE DATABASE statement
type CreateDatabaseStmt struct {
	Name *Identifier
}

// DropDatabaseStmt represents a DROP DATABASE statement
type DropDatabaseStmt struct {
	Name *Identifier
}

// CreateIndexStmt represents a CREATE INDEX statement
type CreateIndexStmt struct {
	TableName   *Identifier
	IndexName   *Identifier
	ColumnNames []*Identifier
	Unique      bool
}

// DropIndexStmt represents a DROP INDEX statement
type DropIndexStmt struct {
	TableName *Identifier
	IndexName *Identifier
}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	TableName   *Identifier
	TableSchema *catalog.TableSchema
}

type DropTableStmt struct {
	TableName *Identifier
}

// UseStmt represents a USE statement
type UseStmt struct {
	DatabaseName *Identifier
}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	TableName   *Identifier
	ColumnNames []*Identifier
	Values      [][]*Literal
}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Distinct        bool
	SelectList      *SelectList
	TableExpression *TableExpression
}

// TableExpression represents a table expression in a SELECT statement
type TableExpression struct {
	FromClause    *FromClause
	WhereClause   *WhereClause
	GroupByClause *GroupByClause
	HavingClause  *HavingClause
}

// FromClause represents a FROM clause in a SELECT statement
type FromClause struct {
	Tables []*Table
}

// Table represents a table in a FROM clause
type Table struct {
	Name *Identifier
}

// WhereClause represents a WHERE clause in a SELECT statement
type WhereClause struct {
	SearchCondition interface{}
}

type ComparisonOperator int

const (
	_ ComparisonOperator = iota
	OP_EQ
	OP_NEQ
	OP_LT
	OP_LTE
	OP_GT
	OP_GTE
)

// getComparisonOperator returns the ComparisonOperator for the given operator
func getComparisonOperator(op string) ComparisonOperator {
	switch op {
	case "=":
		return OP_EQ
	case "<>":
		return OP_NEQ
	case "<":
		return OP_LT
	case "<=":
		return OP_LTE
	case ">":
		return OP_GT
	case ">=":
		return OP_GTE
	}
	return 0
}

// ComparisonPredicate represents a comparison predicate
type ComparisonPredicate struct {
	Left  *ValueExpression
	Op    ComparisonOperator
	Right *ValueExpression
}

// LogicalOperator represents a logical operator
type LogicalOperator int

const (
	_ LogicalOperator = iota
	OP_AND
	OP_OR
	OP_NOT
)

// getLogicalOperator returns the LogicalOperator for the given operator
func getLogicalOperator(op string) LogicalOperator {
	switch op {
	case "AND":
		return OP_AND
	case "OR":
		return OP_OR
	case "NOT":
		return OP_NOT
	}
	return 0
}

// LogicalCondition represents a logical condition
type LogicalCondition struct {
	Left  interface{}
	Op    LogicalOperator
	Right interface{}
}

// GroupByClause represents a GROUP BY clause in a SELECT statement
type GroupByClause struct {
	Columns []*ColumnSpecification
}

// HavingClause represents a HAVING clause in a SELECT statement
type HavingClause struct {
	SearchCondition interface{}
}

// SelectList represents a list of value expressions in a SELECT statement
type SelectList struct {
	Expressions []*ValueExpression
}

// ValueExpression represents a value expression
type ValueExpression struct {
	Value interface{}
	Alias *Literal
}

// Wildcard represents a wildcard in a select list
type Wildcard struct{}

// ColumnSpecification represents a column specification
type ColumnSpecification struct {
	TableName  *Identifier
	ColumnName *Identifier
}

// BinaryExpression represents a binary expression
type BinaryExpression struct {
	Left  interface{}
	Op    BinaryExpressionOperator
	Right interface{}
}

type BinaryExpressionOperator int

const (
	_ BinaryExpressionOperator = iota
	OP_PLUS
	OP_MINUS
	OP_MULT
	OP_DIV
)

// getBinaryExpressionOperator returns the BinaryExpressionOperator for the given operator
func getBinaryExpressionOperator(op string) BinaryExpressionOperator {
	switch op {
	case "+":
		return OP_PLUS
	case "-":
		return OP_MINUS
	case "*":
		return OP_MULT
	case "/":
		return OP_DIV
	}
	return 0
}

// AggregateFunc represents an aggregate function
type AggregateFunc struct {
	FuncName string
	Args     []interface{} // ColumnSpec or  BinaryExpr or AggFunc
}

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Op   string
	Expr interface{}
}

// NotExpr represents a NOT expression
type NotExpr struct {
	Expr interface{}
}

// BetweenPredicate represents a BETWEEN predicate
type BetweenPredicate struct {
	Left  *ValueExpression
	Lower *ValueExpression
	Upper *ValueExpression
}

// InPredicate represents an IN predicate
type InPredicate struct {
	Left   *ValueExpression
	Values []*ValueExpression
}

// LikePredicate represents a LIKE predicate
type LikePredicate struct {
	Left    *ValueExpression
	Pattern *ValueExpression
}

// IsNullPredicate represents an IS NULL predicate
type IsNullPredicate struct {
	Left *ValueExpression
}

// IsNotNullPredicate represents an IS NOT NULL predicate
type IsNotNullPredicate struct {
	Left *ValueExpression
}

// PrintAST prints the AST of a parsed SQL statement in JSON format
func PrintAST(node Node) (string, error) {
	marshalled, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return "", err
	}

	return string(marshalled), nil

}
