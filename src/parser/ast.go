// Package parser asts
// AriaSQL parser asts
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

import "ariasql/catalog"

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

// CreateSchemaStmt represents a CREATE SCHEMA statement
type CreateSchemaStmt struct {
	Name *Identifier
}

// DropSchemaStmt represents a DROP SCHEMA statement
type DropSchemaStmt struct {
	Name *Identifier
}

// CreateIndexStmt represents a CREATE INDEX statement
type CreateIndexStmt struct {
	SchemaName  *Identifier
	TableName   *Identifier
	IndexName   *Identifier
	ColumnNames []*Identifier
	Unique      bool
}

// DropIndexStmt represents a DROP INDEX statement
type DropIndexStmt struct {
	SchemaName *Identifier
	TableName  *Identifier
	IndexName  *Identifier
}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	SchemaName  *Identifier
	TableName   *Identifier
	TableSchema *catalog.TableSchema
}

type DropTableStmt struct {
	SchemaName *Identifier
	TableName  *Identifier
}

// UseStmt represents a USE statement
type UseStmt struct {
	DatabaseName *Identifier
}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	SchemaName  *Identifier
	TableName   *Identifier
	ColumnNames []*Identifier
	Values      [][]*Literal
}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Distinct  bool
	ColumnSet *ColumnSet
	From      *FromClause
	Joins     []*Join
	Where     *WhereClause
	GroupBy   *GroupByClause
	Having    *HavingClause
	OrderBy   *OrderByClause
	Limit     *LimitClause
	Union     *UnionStmt
	Intersect *IntersectStmt
	Except    *ExceptStmt
}

// BinaryExpr represents a binary expression
type BinaryExpr struct {
	Left  interface{} // ColumnSpec or ValueExpr
	Op    string      // +, -, *, /
	Right interface{} // ColumnSpec or ValueExpr
}

// ValueExpr represents a value expression
type ValueExpr struct {
	Value interface{}
	Alias *Identifier
}

// ColumnSpec represents a column specification
type ColumnSpec struct {
	SchemaName *Identifier
	TableName  *Identifier
	ColumnName *Identifier
	Alias      *Identifier
}

// ColumnSet represents a set of columns or expressions
type ColumnSet struct {
	Exprs []interface{} // ColumnSpec or ValueExpr
}

// AggFunc represents an aggregate function
type AggFunc struct {
	FuncName string
	Args     []interface{} // ColumnSpec or ValueExpr or Literal or BinaryExpr or AggFunc
}

// Func represents a function like UPPER or LOWER
type Func struct {
	FuncName string
	Expr     interface{} // ColumnSpec or ValueExpr
}

type JoinType int

const (
	// InnerJoin represents an inner join
	InnerJoin JoinType = iota
	// LeftJoin represents a left join
	LeftJoin
	// RightJoin represents a right join
	RightJoin
	// FullJoin represents a full join
	FullJoin
	// CrossJoin represents a cross join
	CrossJoin
	// NaturalJoin represents a natural join
	NaturalJoin
)

// Join represents a join
type Join struct {
	LeftTable  *Table
	RightTable *Table
	JoinType   JoinType
	Cond       interface{} // ComparisonPredicate
}

type ComparisonOperator int

const (
	// Eq represents the = operator
	Eq ComparisonOperator = iota
	// Ne represents the !=/<> operator
	Ne
	// Lt represents the < operator
	Lt
	// Le represents the <= operator
	Le
	// Gt represents the > operator
	Gt
	// Ge represents the >= operator
	Ge
)

// ComparisonPredicate represents a comparison predicate
type ComparisonPredicate struct {
	LeftExpr  interface{} // ColumnSpec or Literal
	RightExpr interface{} // ColumnSpec or Literal
	Operator  ComparisonOperator
}

// LogicalOperator represents a logical operator
type LogicalOperator int

const (
	// And represents the AND operator
	And LogicalOperator = iota
	// Or represents the OR operator
	Or
	// Not represents the NOT operator
	Not
)

// LogicalCondition represents a logical condition
type LogicalCondition struct {
	LeftCond  interface{} // Predicate or LogicalCondition
	RightCond interface{} // Predicate or LogicalCondition
	Operator  LogicalOperator
}

// BetweenPredicate represents a BETWEEN predicate
type BetweenPredicate struct {
	Expr  interface{} // ColumnSpec or Literal
	Lower interface{} // ColumnSpec or Literal
	Upper interface{} // ColumnSpec or Literal
}

// InPredicate represents an IN predicate
type InPredicate struct {
	Expr     interface{}   // ColumnSpec or Literal
	Values   []interface{} // ColumnSpec or Literal
	Subquery *SelectStmt
}

// LikePredicate represents a LIKE predicate
type LikePredicate struct {
	Expr    interface{} // ColumnSpec or Literal
	Pattern interface{} // ColumnSpec or Literal
}

// IsNullPredicate represents an IS NULL predicate
type IsNullPredicate struct {
	Expr interface{} // ColumnSpec or Literal
}

// ExistsPredicate represents an EXISTS predicate
type ExistsPredicate struct {
	SelectStmt *SelectStmt
}

// AnyPredicate represents an ANY predicate
type AnyPredicate struct {
	SelectStmt *SelectStmt
}

// AllPredicate represents an ALL predicate
type AllPredicate struct {
	SelectStmt *SelectStmt
}

// SomePredicate represents a SOME predicate
type SomePredicate struct {
	SelectStmt *SelectStmt
}

// NotPredicate represents a NOT predicate
type NotPredicate struct {
	Expr interface{} // Predicate
}

// IsNotNullPredicate represents an IS NOT NULL predicate
type IsNotNullPredicate struct {
	Expr interface{} // ColumnSpec or Literal
}

// HavingClause represents a HAVING clause
type HavingClause struct {
	Cond interface{} // Predicate or LogicalCondition or BinaryExpr
}

// GroupByClause represents a GROUP BY clause
type GroupByClause struct {
	Columns []interface{} // ColumnSpec or ValueExpr
}

// OrderByClause represents an ORDER BY clause
type OrderByClause struct {
	Columns []interface{}    // ColumnSpec or ValueExpr
	Dir     OrderByDirection // ASC or DESC
}

type OrderByDirection int

const (
	// Asc represents the ASC direction
	Asc OrderByDirection = iota
	// Desc represents the DESC direction
	Desc
)

// LimitClause represents a LIMIT clause
type LimitClause struct {
	Offset int
	Count  int
}

// UnionStmt represents a UNION statement
type UnionStmt struct {
	SelectStmt *SelectStmt
	All        bool
}

// IntersectStmt represents an INTERSECT statement
type IntersectStmt struct {
	SelectStmt *SelectStmt
	All        bool
}

// ExceptStmt represents an EXCEPT statement
type ExceptStmt struct {
	SelectStmt *SelectStmt
	All        bool
}

// FromClause represents a FROM clause
type FromClause struct {
	Tables []*Table
}

// Table represents a table
type Table struct {
	SchemaName *Identifier
	TableName  *Identifier
	Alias      *Identifier
}

// WhereClause represents a WHERE clause
type WhereClause struct {
	Cond interface{} // Predicate or LogicalCondition or BinaryExpr
}

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Op   string
	Expr interface{}
}
