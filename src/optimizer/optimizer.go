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
func Optimize(ast parser.Node, cat *catalog.Catalog) *PhysicalPlan {

	switch ast.(type) {
	case *parser.CreateDatabaseStmt:
		return OpCreateDatabaseStmt(ast.(*parser.CreateDatabaseStmt), cat)
	case *parser.CreateSchemaStmt:
		return OpCreateSchemaStmt(ast.(*parser.CreateSchemaStmt), cat)
	case *parser.CreateTableStmt:
		return OpCreateTableStmt(ast.(*parser.CreateTableStmt), cat)
	case *parser.CreateIndexStmt:
		return OpCreateIndexStmt(ast.(*parser.CreateIndexStmt), cat)
	case *parser.UseStmt:
		return OpUseStmt(ast.(*parser.UseStmt), cat)
	case *parser.InsertStmt:
		return OpInsertStmt(ast.(*parser.InsertStmt), cat)
	case *parser.SelectStmt:
		return OpSelectStmt(ast.(*parser.SelectStmt), cat)

	}

	return &PhysicalPlan{}
}

// OpCreateDatabaseStmt optimizes the CreateDatabaseStmt
func OpCreateDatabaseStmt(stmt *parser.CreateDatabaseStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create database
}

// OpCreateSchemaStmt optimizes the CreateSchemaStmt
func OpCreateSchemaStmt(stmt *parser.CreateSchemaStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create schema
}

// OpCreateIndexStmt optimizes the CreateIndexStmt
func OpCreateIndexStmt(stmt *parser.CreateIndexStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create index
}

// OpCreateTableStmt optimizes the CreateTableStmt
func OpCreateTableStmt(stmt *parser.CreateTableStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for create table
}

// OpUseStmt optimizes the UseStmt
func OpUseStmt(stmt *parser.UseStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for use
}

// OpInsertStmt optimizes the InsertStmt
func OpInsertStmt(stmt *parser.InsertStmt, cat *catalog.Catalog) *PhysicalPlan {
	return &PhysicalPlan{Plan: stmt} // no optimization needed for insert
}

func OpSelectStmt(stmt *parser.SelectStmt, cat *catalog.Catalog) *PhysicalPlan {
	// Generate plans based on the query
	// Once we have the plans, we can choose the best one based on the cost
	plans := []*PhysicalPlan{}

	// We can have multiple plans for a query

	best := getBestPlan(plans)
	if best == nil {
		return nil
	}

	return &PhysicalPlan{Plan: best}
}

// getBestPlan returns the best plan from a list of optimized plans for a given query
func getBestPlan(plans OptimizedPlans) *PhysicalPlan {
	// We return the plan with the lowest cost

	currPlan := plans[0]

	for _, plan := range plans {
		if plan.PlanCost.Cost < currPlan.PlanCost.Cost {
			currPlan = plan
		}
	}

	return currPlan
}
