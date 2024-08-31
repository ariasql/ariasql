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
	"errors"
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
	case *optimizer.CreateDatabasePlan:
		return e.aria.Catalog.CreateDatabase(plan.Plan.(*optimizer.CreateDatabasePlan).DatabaseName)
	case *optimizer.CreateSchemaPlan:
		if e.channel.Database == nil {
			return errors.New("no database selected")
		}

		return e.channel.Database.CreateSchema(plan.Plan.(*optimizer.CreateSchemaPlan).SchemaName)
	case *optimizer.CreateTablePlan:
		if e.channel.Database == nil {
			return errors.New("no database selected")
		}

		return plan.Plan.(*optimizer.CreateTablePlan).Schema.CreateTable(plan.Plan.(*optimizer.CreateTablePlan).TableName, plan.Plan.(*optimizer.CreateTablePlan).TableSchema)
	case *optimizer.CreateIndexPlan:
		if e.channel.Database == nil {
			return errors.New("no database selected")
		}

		return plan.Plan.(*optimizer.CreateIndexPlan).Table.CreateIndex(plan.Plan.(*optimizer.CreateIndexPlan).IndexName, plan.Plan.(*optimizer.CreateIndexPlan).ColumnNames, plan.Plan.(*optimizer.CreateIndexPlan).Unique)
	case *optimizer.UsePlan:
		e.channel.Database = plan.Plan.(*optimizer.UsePlan).Database
		return nil
	case *optimizer.InsertPlan:
		if e.channel.Database == nil {
			return errors.New("no database selected")
		}

		return plan.Plan.(*optimizer.InsertPlan).Table.Insert(plan.Plan.(*optimizer.InsertPlan).Rows)
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
