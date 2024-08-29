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

// CreateSchemaStmt represents a CREATE SCHEMA statement
type CreateSchemaStmt struct {
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

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	SchemaName  *Identifier
	TableName   *Identifier
	TableSchema *catalog.TableSchema
}
