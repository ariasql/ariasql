// Package executor
// AriaSQL executor package
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
package executor

import (
	"ariasql/core"
	"ariasql/optimizer"
	"ariasql/parser"
	"errors"
	"log"
)

// Executor is an AriaSQL query executor
type Executor struct {
	aria           *core.AriaSQL // AriaSQL instance pointer
	channel        *core.Channel // Channel to execute the query on
	responseBuffer []byte        // Response buffer
}

// NewExecutor creates a new Executor
func NewExecutor(aria *core.AriaSQL, channel *core.Channel) *Executor {
	return &Executor{
		aria:    aria,
		channel: channel,
	}
}

// Execute executes the query plan
func (e *Executor) Execute(plan *optimizer.PhysicalPlan) error {
	switch plan.Plan.(type) {
	case *parser.CreateDatabaseStmt: // Create database statement
		return e.aria.Catalog.CreateDatabase(plan.Plan.(*parser.CreateDatabaseStmt).Name.Value)
	case *parser.CreateSchemaStmt: // Create schema statement
		return e.channel.Database.CreateSchema(plan.Plan.(*parser.CreateSchemaStmt).Name.Value)
	case *parser.CreateTableStmt: // Create table statement
		sch := e.channel.Database.GetSchema(plan.Plan.(*parser.CreateTableStmt).SchemaName.Value)
		if sch == nil {
			return errors.New("schema does not exist")
		}

		return sch.CreateTable(plan.Plan.(*parser.CreateTableStmt).TableName.Value, plan.Plan.(*parser.CreateTableStmt).TableSchema)
	case *parser.CreateIndexStmt: // Create index statement, unique or non-unique
		sch := e.channel.Database.GetSchema(plan.Plan.(*parser.CreateIndexStmt).SchemaName.Value)
		if sch == nil {
			return errors.New("schema does not exist")
		}

		tbl := sch.GetTable(plan.Plan.(*parser.CreateIndexStmt).TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		var columns []string

		for _, col := range plan.Plan.(*parser.CreateIndexStmt).ColumnNames {
			columns = append(columns, col.Value)
		}

		return tbl.CreateIndex(plan.Plan.(*parser.CreateIndexStmt).IndexName.Value, columns, plan.Plan.(*parser.CreateIndexStmt).Unique)
	case *parser.UseStmt: // Use statement, sets the current database for the channel
		db := e.aria.Catalog.GetDatabase(plan.Plan.(*parser.UseStmt).DatabaseName.Value)
		if db == nil {
			return errors.New("database does not exist")
		}

		e.channel.Database = db

		return nil

	case *parser.InsertStmt: // Insert statement, handles multiple rows
		sch := e.channel.Database.GetSchema(plan.Plan.(*parser.InsertStmt).SchemaName.Value)
		if sch == nil {
			return errors.New("schema does not exist")
		}

		tbl := sch.GetTable(plan.Plan.(*parser.InsertStmt).TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		rows := []map[string]interface{}{}

		for _, row := range plan.Plan.(*parser.InsertStmt).Values {
			r := make(map[string]interface{})
			for i, col := range plan.Plan.(*parser.InsertStmt).ColumnNames {
				r[col.Value] = row[i].Value
			}
			rows = append(rows, r)
		}

		return tbl.Insert(rows)

	case *optimizer.SelectPlan: // Select statement

		log.Println("exec	 select statement")

		return nil

	}

	return errors.New("invalid plan")
}

// GetResponseBuff returns the response buffer
func (e *Executor) GetResponseBuff() []byte {
	return e.responseBuffer
}

// Clear clears the response buffer
func (e *Executor) Clear() {
	e.responseBuffer = []byte{}
}
