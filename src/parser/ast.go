// Package parser ast
// AriaSQL parser ast package
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
	Values      [][]*Literal
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
	Join  JoinType
}

// JoinType represents a join type
type JoinType int

const (
	_            JoinType = iota
	JOIN                  // INNER JOIN
	LEFT_JOIN             // LEFT JOIN
	RIGHT_JOIN            // RIGHT JOIN
	CROSS_JOIN            // CROSS JOIN
	NATURAL_JOIN          // NATURAL JOIN
)

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
	Body       []Statement
}

// Parameter represents a parameter in a procedure
type Parameter struct {
	Name     *Identifier
	DataType *Identifier
}

/*
CREATE PROCEDURE test_procedure
    @param1 INT,
    @param2 CHAR(20)
AS
BEGIN
    SELECT *
    FROM test_table
    WHERE col1 = @param1 AND col2 = @param2;
END;

-- Calling the stored procedure
EXEC test_procedure @param1 = 1, @param2 = 'test';
*/

// ExecStmt represents a CALL statement
type ExecStmt struct {
	Name *Identifier
	Args []*Literal
} // i.e. EXEC test_procedure @param1 = 1, @param2 = 'test';

// PrintAST prints the AST of a parsed SQL statement in JSON format
func PrintAST(node Node) (string, error) {
	marshalled, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return "", err
	}

	return string(marshalled), nil

}
