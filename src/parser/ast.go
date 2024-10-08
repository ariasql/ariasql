// Package parser ast (abstract syntax trees)
// AriaSQL parser ast package is a collection of types that represent the abstract syntax tree of a parsed SQL statement.
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
	"ariasql/shared"
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
	Compress    bool
	Encrypt     bool
	EncryptKey  *Literal
}

// DropTableStmt represents a DROP TABLE statement
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
	Values      [][]interface{}
}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Distinct        bool
	SelectList      *SelectList
	TableExpression *TableExpression
	Union           *SelectStmt
	UnionAll        bool
}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	TableName   *Identifier
	SetClause   []*SetClause
	WhereClause *WhereClause
}

// SetClause represents a SET clause in an UPDATE statement
type SetClause struct {
	Column *Identifier
	Value  *Literal
}

// DeleteStmt represents a DELETE statement
type DeleteStmt struct {
	TableName   *Identifier
	WhereClause *WhereClause
}

// TableExpression represents a table expression in a SELECT statement
type TableExpression struct {
	FromClause    *FromClause
	WhereClause   *WhereClause
	GroupByClause *GroupByClause
	HavingClause  *HavingClause
	OrderByClause *OrderByClause
	LimitClause   *LimitClause
}

// FromClause represents a FROM clause in a SELECT statement
type FromClause struct {
	Tables []*Table
}

// Table represents a table in a FROM clause
type Table struct {
	Name  *Identifier
	Alias *Identifier // i.e. AS alias
}

// WhereClause represents a WHERE clause in a SELECT statement
type WhereClause struct {
	SearchCondition interface{}
}

// ComparisonOperator represents a comparison operator
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
	GroupByExpressions []*ValueExpression
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
	Alias *Identifier
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

// BinaryExpressionOperator represents a binary expression operator
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

type IsPredicate struct {
	Left *ValueExpression
	Null bool
}

// ExistsPredicate represents an EXISTS predicate
type ExistsPredicate struct {
	Tables []*Table
	Expr   *ValueExpression
}

// OrderByOrder represents the order of an ORDER BY clause
type OrderByOrder int

const (
	_ OrderByOrder = iota
	ASC
	DESC
)

// OrderByClause represents an ORDER BY clause in a SELECT statement
type OrderByClause struct {
	OrderByExpressions []*ValueExpression
	Order              OrderByOrder
}

// LimitClause represents a LIMIT clause in a SELECT statement
type LimitClause struct {
	Offset *Literal
	Count  *Literal
}

// BeginStmt represents a BEGIN statement
type BeginStmt struct{}

// CommitStmt represents a COMMIT statement
type CommitStmt struct{}

// RollbackStmt represents a ROLLBACK statement
type RollbackStmt struct{}

// GrantStmt represents a GRANT statement
type GrantStmt struct {
	PrivilegeDefinition *PrivilegeDefinition
}

// RevokeStmt represents a REVOKE statement
type RevokeStmt struct {
	PrivilegeDefinition *PrivilegeDefinition
}

// PrivilegeDefinition Privilege represents a privilege
type PrivilegeDefinition struct {
	Actions []shared.PrivilegeAction
	Object  *Identifier // can be dbname.* or dbname.tablename, or *
	Grantee *Identifier // User
	Revokee *Identifier // User
}

// CreateUserStmt represents a CREATE USER statement
type CreateUserStmt struct {
	Username *Identifier
	Password *Literal
}

// DropUserStmt represents a DROP USER statement
type DropUserStmt struct {
	Username *Identifier
}

type ShowType int

const (
	_ ShowType = iota
	SHOW_DATABASES
	SHOW_TABLES
	SHOW_USERS
	SHOW_INDEXES
	SHOW_GRANTS
)

// ShowStmt represents a SHOW statement
type ShowStmt struct {
	ShowType ShowType
	For      *Identifier
	From     *Identifier
}

// AlterTableStmt represents an ALTER TABLE statement
type AlterTableStmt struct {
	TableName        *Identifier               // Table name
	ColumnName       *Identifier               // Column name
	ColumnDefinition *catalog.ColumnDefinition // Column definition
}

type AlterUserSetType int

const (
	_ AlterUserSetType = iota
	ALTER_USER_SET_PASSWORD
	ALTER_USER_SET_USERNAME
)

// AlterUserStmt represents an ALTER USER statement
type AlterUserStmt struct {
	SetType  AlterUserSetType
	Username *Identifier
	Value    *Literal
}

// ProcedureStmt represents a CREATE PROCEDURE statement
type ProcedureStmt struct {
	Name       *Identifier
	Parameters []*Identifier
	Body       []interface{}
}

// Parameter represents a parameter in a procedure
type Parameter struct {
	Name      *Identifier
	DataType  *Identifier
	Length    *Literal
	Precision *Literal
	Scale     *Literal
}

// PrintAST prints the AST of a parsed SQL statement in JSON format
func PrintAST(node Node) (string, error) {
	marshalled, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return "", err
	}

	return string(marshalled), nil

}

// System Functions

// UpperFunc represents an UPPER function
type UpperFunc struct {
	Arg interface{} // Can be a column name or a string
}

// LowerFunc represents a LOWER function
type LowerFunc struct {
	Arg interface{} // Can be a column name or a string
}

// CastFunc represents a CAST function
type CastFunc struct {
	Expr     interface{}
	DataType *Identifier
}

// CoalesceFunc represents a COALESCE function
// i.e COALESCE(column_name, 0)
type CoalesceFunc struct {
	Args  []interface{} // Can be a column name
	Value interface{}   // Default value
}

// ReverseFunc represents a REVERSE function
type ReverseFunc struct {
	Arg interface{} // Can be a column name or a string
}

// RoundFunc represents a ROUND function
type RoundFunc struct {
	Arg interface{} // Can be a column name or a string
}

// PositionFunc represents a POSITION function
type PositionFunc struct {
	Arg interface{} // Can be a column name or a string
	In  interface{} // Can be a column name or a string
}

// LengthFunc represents a LENGTH function
type LengthFunc struct {
	Arg interface{} // Can be a column name or a string
}

// TrimFunc represents a TRIM function
type TrimFunc struct {
	Arg interface{} // Can be a column name or a string
}

// SubstrFunc represents a SUBSTRING function
type SubstrFunc struct {
	Arg      interface{} // Can be a column name or a string
	StartPos *Literal
	Length   *Literal
}

// ConcatFunc represents a CONCAT function
type ConcatFunc struct {
	Args []interface{} // Can be a column name or a string
}

// CaseExpr represents a CASE expression
type CaseExpr struct {
	WhenClauses []*WhenClause
	ElseClause  interface{}
}

// WhenClause represents a WHEN clause
type WhenClause struct {
	Condition interface{}
	Result    interface{}
}

// ElseClause represents an ELSE clause
type ElseClause struct {
	Result interface{}
}

// Cursor structures

// DeclareStmt declares a cursor variable or cursor
type DeclareStmt struct {
	CursorName             *Identifier
	CursorStmt             *SelectStmt
	CursorVariableName     *Identifier // @variable_name
	CursorVariableDataType *Identifier // variable data type
}

// Variable represents a variable
type Variable struct {
	VariableName *Identifier
}

// OpenStmt opens a cursor
type OpenStmt struct {
	CursorName *Identifier
}

// FetchStmt fetches a row from a cursor
type FetchStmt struct {
	CursorName *Identifier
	Into       []*Identifier
}

// WhileStmt represents a WHILE loop
type WhileStmt struct {
	Stmts       *BeginEndBlock
	FetchStatus *Literal
}

// PrintStmt represents a PRINT statement
type PrintStmt struct {
	Expr interface{}
}

// BeginEndBlock represents a BEGIN...END block
type BeginEndBlock struct {
	Stmts []interface{}
}

// IfStmt represents an IF statement
type IfStmt struct {
	Condition interface{}
}

// ElseIfStmt represents an ELSE IF statement
type ElseIfStmt struct {
	Condition interface{}
}

// ExitStmt represents an EXIT statement within a loop
type ExitStmt struct{}

// BreakStmt represents a BREAK statement within a loop
type BreakStmt struct{}

// ReturnStmt represents a RETURN statement like RETURN 1;
type ReturnStmt struct {
	Expr interface{} // Should be literal
}

// SetStmt represents a SET statement like SET @variable_name = 1;
type SetStmt struct {
	Variable *Identifier // variable name
	Value    interface{} // Should be literal
}

// CloseStmt represents a CLOSE statement
type CloseStmt struct {
	CursorName *Identifier // cursor name
}

// DeallocateStmt represents a DEALLOCATE statement
type DeallocateStmt struct {
	CursorName         *Identifier // cursor name
	CursorVariableName *Identifier // cursor variable name
}

// CreateProcedureStmt represents a CREATE PROCEDURE statement
type CreateProcedureStmt struct {
	Procedure *Procedure // procedure definition
}

// ExecStmt represents a EXEC statement
// i.e EXEC procedure_name;
type ExecStmt struct {
	ProcedureName *Identifier // procedure name
	Args          []interface{}
}

// Procedure represents a procedure
type Procedure struct {
	Name       *Identifier    // procedure name
	Parameters []*Parameter   // procedure parameters
	Body       *BeginEndBlock // procedure body
}

// DropProcedureStmt represents a DROP PROCEDURE statement
type DropProcedureStmt struct {
	ProcedureName *Identifier // procedure name
}

// ExplainStmt represents an EXPLAIN statement
type ExplainStmt struct {
	Stmt interface{} // Can be SelectStmt, UpdateStmt, DeleteStmt
}
