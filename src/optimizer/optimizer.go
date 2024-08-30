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
	"log"
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

// SelectPlan is the plan for a select statement
type SelectPlan struct {
	IndexScan       *IndexScan       // uses an index to find the relevant rows more quickly than a table scan.
	TableScan       *TableScan       //  reads the entire table to find the relevant rows.
	NestedLoopsJoin *NestedLoopsJoin // For each row in the first table, the DBMS scans the entire second table. This is efficient when one of the tables is much smaller than the other.
	HashJoin        *HashJoin        // builds a hash table of the smaller table, then scans the larger table and uses the hash table to find matching rows. This is efficient when the tables are of similar size.
	SortMergeJoin   *SortMergeJoin   // sorts both tables on the join column, then merges them together.
	Materialize     *Materialize     // creates a temporary table in memory to hold and quickly access the results of a subquery.
}

// UpdatePlan is the plan for an update statement
type UpdatePlan struct {
	IndexScan *IndexScan
	TableScan *TableScan
}

// DeletePlan is the plan for a delete statement
type DeletePlan struct {
	IndexScan *IndexScan
	TableScan *TableScan
}

// NestedLoopsJoin is the plan for a nested loops join
type NestedLoopsJoin struct {
	LeftPlan  *PhysicalPlan // the left plan
	RightPlan *PhysicalPlan // the right plan
}

// HashJoin is the plan for a hash join
type HashJoin struct {
	LeftPlan  *PhysicalPlan // the left plan
	RightPlan *PhysicalPlan // the right plan
}

// SortMergeJoin is the plan for a sort merge join
type SortMergeJoin struct {
	LeftPlan  *PhysicalPlan // the left plan
	RightPlan *PhysicalPlan // the right plan
}

// Materialize is the plan for a materialize operation
type Materialize struct {
	SubqueryPlan *PhysicalPlan // the plan for the subquery
}

// TableScan is the plan for a table scan
type TableScan struct {
	TableName string
}

// IndexScan is the plan for an index scan
type IndexScan struct {
	IndexName string
	TableName string
}

func OpSelectStmt(stmt *parser.SelectStmt, cat *catalog.Catalog) *PhysicalPlan {
	log.Println("optimizing select statement")
	// Generate plans based on the query
	// Once we have the plans, we can choose the best one based on the cost
	//plans := []*PhysicalPlan{}

	// We create multiple plans based on the query

	if stmt.From != nil {
		// We have a FROM clause
		// We can have multiple tables in the FROM clause
		ast, err := parser.PrintAST(stmt.From)
		if err != nil {
			return nil
		}
		log.Println(ast)

		// can be schema.table AS alias, schema.table, table AS alias, ...
		// if we have above we don't parse the stmt.Join

		if len(stmt.From.Tables) > 1 {
			// This is a classic join
			// i.e SELECT * FROM s.table1, s.table2 WHERE s.table1.id = s.table2.id
			// OR SELECT * FROM s.table1 AS a, s.table2 AS b WHERE a.id = b.id
		} else if len(stmt.From.Tables) == 1 {
			// Look for joins
			// i.e SELECT * FROM s.table1 JOIN s.table2 ON s.table1.id = s.table2.id

			// We have a single table in the FROM clause
			// We can have JOIN clauses

			if stmt.Joins != nil {
				// We have JOIN clauses
				// We can have multiple JOIN clauses

				// We can have INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN, NATURAL JOIN
				// We can have multiple JOIN clauses
				// We can have a combination of JOIN clauses
				// We can have a combination of JOIN clauses and WHERE clauses

			} else {
				// We have a single table in the FROM clause, no JOIN clauses
				// We can have WHERE clauses
				// We can have GROUP BY clauses
				// We can have ORDER BY clauses
				// We can have LIMIT clauses
				// We can have OFFSET clauses
				// We can have HAVING clauses
				// We can have UNION clauses
				// We can have INTERSECT clauses
				// We can have EXCEPT clauses

				// We can have subqueries

				// Depending on the columns and tables in the SELECT clause, we can have different plans
				// For example we can look if the left table is smaller than the right table, if so we can use a nested loops join
				// If the tables are of similar size, we can use a hash join
				// If there is lots of data, we can use a sort merge join, we batch sort in memory and process the data in chunks
				// If we have an index on the join column, we can use an index scan

				// We can have a table scan if there is no index on the join column
			}
		}
	}

	//best := getBestPlan(plans)
	//if best == nil {
	//	return nil
	//}

	return &PhysicalPlan{Plan: &SelectPlan{}}
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
