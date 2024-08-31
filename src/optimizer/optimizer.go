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
	"ariasql/core"
	"ariasql/parser"
	"errors"
)

// PhysicalPlan is the final plan that will be executed by the executor
type PhysicalPlan struct {
	Plan     interface{}
	PlanCost *PlanCost
}

// OptimizedPlans is a list of optimized plans
type OptimizedPlans []*PhysicalPlan

// PlanCost is the cost of a plan
type PlanCost struct {
	// The cost of the plan
	Cost float64 // lower is better
}

// Optimize optimizes the AST and returns a PhysicalPlan
// Not every statement needs optimization, so we return the AST as is, the executor will handle the execution of the statement
func Optimize(ast parser.Node, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {

	switch ast.(type) {
	case *parser.CreateDatabaseStmt:
		return OpCreateDatabaseStmt(ast.(*parser.CreateDatabaseStmt), cat, ch)
	case *parser.CreateSchemaStmt:
		return OpCreateSchemaStmt(ast.(*parser.CreateSchemaStmt), cat, ch)
	case *parser.CreateTableStmt:
		return OpCreateTableStmt(ast.(*parser.CreateTableStmt), cat, ch)
	case *parser.CreateIndexStmt:
		return OpCreateIndexStmt(ast.(*parser.CreateIndexStmt), cat, ch)
	case *parser.UseStmt:
		return OpUseStmt(ast.(*parser.UseStmt), cat, ch)
	case *parser.InsertStmt:
		return OpInsertStmt(ast.(*parser.InsertStmt), cat, ch)

	}

	return &PhysicalPlan{}, errors.New("no plan found")
}

// CreateDatabasePlan is the plan for a create database statement
type CreateDatabasePlan struct {
	DatabaseName string
}

// OpCreateDatabaseStmt optimizes the CreateDatabaseStmt
func OpCreateDatabaseStmt(stmt *parser.CreateDatabaseStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	if stmt.Name.Value == "" {
		return nil, errors.New("database name cannot be empty")
	}

	return &PhysicalPlan{Plan: &CreateDatabasePlan{
		DatabaseName: stmt.Name.Value,
	}}, nil
}

// CreateSchemaPlan is the plan for a create schema statement
type CreateSchemaPlan struct {
	Database   *catalog.Database
	SchemaName string
}

// OpCreateSchemaStmt optimizes the CreateSchemaStmt
func OpCreateSchemaStmt(stmt *parser.CreateSchemaStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	if stmt.Name.Value == "" {
		return nil, errors.New("schema name cannot be empty")
	}

	if ch.Database == nil {
		return nil, errors.New("no database selected")
	}

	return &PhysicalPlan{Plan: &CreateSchemaPlan{
		Database:   ch.Database,
		SchemaName: stmt.Name.Value,
	}}, nil
}

// CreateIndexPlan is the plan for a create index statement
type CreateIndexPlan struct {
	Table       *catalog.Table
	IndexName   string
	ColumnNames []string
	Unique      bool
}

// OpCreateIndexStmt optimizes the CreateIndexStmt
func OpCreateIndexStmt(stmt *parser.CreateIndexStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	if stmt.IndexName.Value == "" {
		return nil, errors.New("index name cannot be empty")
	}

	if stmt.TableName.Value == "" {
		return nil, errors.New("table name cannot be empty")
	}

	if len(stmt.ColumnNames) == 0 {
		return nil, errors.New("no columns specified for index")
	}

	if ch.Database == nil {
		return nil, errors.New("no database selected")
	}

	sch := ch.Database.GetSchema(stmt.SchemaName.Value)
	if sch == nil {
		return nil, errors.New("schema does not exist")
	}

	tbl := sch.GetTable(stmt.TableName.Value)
	if tbl == nil {
		return nil, errors.New("table does not exist")

	}

	plan := &CreateIndexPlan{
		Table:       tbl,
		IndexName:   stmt.IndexName.Value,
		ColumnNames: []string{},
		Unique:      false,
	}

	for _, col := range stmt.ColumnNames {
		plan.ColumnNames = append(plan.ColumnNames, col.Value)
	}

	if stmt.Unique {
		plan.Unique = true
	}

	return &PhysicalPlan{Plan: plan}, nil
}

// CreateTablePlan is the plan for a create table statement
type CreateTablePlan struct {
	Schema      *catalog.Schema
	TableName   string
	TableSchema *catalog.TableSchema
}

// OpCreateTableStmt optimizes the CreateTableStmt
func OpCreateTableStmt(stmt *parser.CreateTableStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	if stmt.TableName.Value == "" {
		return nil, errors.New("table name cannot be empty")
	}

	if stmt.TableSchema == nil {
		return nil, errors.New("table schema cannot be empty")
	}

	if ch.Database == nil {
		return nil, errors.New("no database selected")
	}

	sch := ch.Database.GetSchema(stmt.SchemaName.Value)
	if sch == nil {
		return nil, errors.New("schema does not exist")

	}

	return &PhysicalPlan{Plan: &CreateTablePlan{
		Schema:      sch,
		TableName:   stmt.TableName.Value,
		TableSchema: stmt.TableSchema,
	}}, nil
}

// UsePlan is the plan for a use statement
type UsePlan struct {
	Database *catalog.Database
}

// OpUseStmt optimizes the UseStmt
func OpUseStmt(stmt *parser.UseStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	db := cat.GetDatabase(stmt.DatabaseName.Value)

	if db == nil {
		return nil, errors.New("database does not exist")
	}

	return &PhysicalPlan{Plan: &UsePlan{
		Database: db,
	}}, nil
}

// InsertPlan is the plan for an insert statement
type InsertPlan struct {
	Table *catalog.Table
	Rows  []map[string]interface{}
}

// OpInsertStmt optimizes the InsertStmt
func OpInsertStmt(stmt *parser.InsertStmt, cat *catalog.Catalog, ch *core.Channel) (*PhysicalPlan, error) {
	plan := &InsertPlan{}

	sch := ch.Database.GetSchema(stmt.SchemaName.Value)
	if sch == nil {
		return nil, errors.New("schema does not exist")
	}

	if stmt.TableName == nil {
		return nil, errors.New("table name cannot be empty")
	}

	tbl := sch.GetTable(stmt.TableName.Value)

	if tbl == nil {
		return nil, errors.New("table does not exist")
	}

	plan.Table = tbl

	rows := []map[string]interface{}{}

	for _, row := range stmt.Values {
		r := make(map[string]interface{})
		for i, col := range stmt.ColumnNames {
			r[col.Value] = row[i].Value
		}
		rows = append(rows, r)
	}

	if len(rows) == 0 {
		return nil, errors.New("no rows to insert")
	}

	plan.Rows = rows

	return &PhysicalPlan{Plan: plan}, nil
}
