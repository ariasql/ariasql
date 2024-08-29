// Package optimizer
// AriaSQL query optimizer
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
package optimizer

import (
	"ariasql/catalog"
	"ariasql/parser"
)

// PhysicalPlan is the final plan that will be executed by the executor
type PhysicalPlan struct {
	Plan interface{}
}

// Optimize optimizes the AST and returns a PhysicalPlan
// Not every statement needs optimization, so we return the AST as is, the executor will handle the execution of the statement
func Optimize(ast parser.Node, cat *catalog.Catalog) *PhysicalPlan {

	switch ast.(type) {
	case *parser.CreateDatabaseStmt:
		return OptimizeCreateDatabaseStmt(ast.(*parser.CreateDatabaseStmt), cat)
	case *parser.CreateSchemaStmt:
		return OptimizeCreateSchemaStmt(ast.(*parser.CreateSchemaStmt), cat)
	case *parser.CreateTableStmt:
		return OptimizeCreateTableStmt(ast.(*parser.CreateTableStmt), cat)
	case *parser.CreateIndexStmt:
		return OptimizeCreateIndexStmt(ast.(*parser.CreateIndexStmt), cat)
	case *parser.UseStmt:
		return OptimizeUseStmt(ast.(*parser.UseStmt), cat)
	case *parser.InsertStmt:
		return OptimizeInsertStmt(ast.(*parser.InsertStmt), cat)

	}

	return &PhysicalPlan{}
}

// OptimizeCreateDatabaseStmt optimizes the CreateDatabaseStmt
func OptimizeCreateDatabaseStmt(stmt *parser.CreateDatabaseStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create database
}

// OptimizeCreateSchemaStmt optimizes the CreateSchemaStmt
func OptimizeCreateSchemaStmt(stmt *parser.CreateSchemaStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create schema
}

// OptimizeCreateIndexStmt optimizes the CreateIndexStmt
func OptimizeCreateIndexStmt(stmt *parser.CreateIndexStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create index
}

// OptimizeCreateTableStmt optimizes the CreateTableStmt
func OptimizeCreateTableStmt(stmt *parser.CreateTableStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create table
}

// OptimizeUseStmt optimizes the UseStmt
func OptimizeUseStmt(stmt *parser.UseStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for use
}

// OptimizeInsertStmt optimizes the InsertStmt
func OptimizeInsertStmt(stmt *parser.InsertStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for insert
}
