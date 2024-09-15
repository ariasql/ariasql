// Package executor
// Copyright (C) AriaSQL
// Author(s): Alex Gaetano Padula
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
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/parser"
	"ariasql/shared"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Executor is the main executor structure
type Executor struct {
	aria             *core.AriaSQL        // AriaSQL instance pointer
	ch               *core.Channel        // Channel pointer
	json             bool                 // Enable JSON output, default is false, set by client from server usually
	recover          bool                 // Recover flag
	Transaction      *Transaction         // Transaction statements
	TransactionBegun bool                 // Transaction begun
	ResultSetBuffer  []byte               // Result set buffer
	vars             map[string]*Variable // Defined variables
	cursors          map[string]*Cursor   // Allocated cursors
	fetchStatus      atomic.Int32         // Fetch status
	plan             *Plan                // Execution plan
	explaining       bool                 // Explaining flag, populates plan
}

// Variable struct represents a variable on the executor
type Variable struct {
	Value    interface{} // The value of the variable
	DataType string      // The data type of the variable
}

// Cursor represents a cursor
type Cursor struct {
	name      string             // the name of the cursor
	pos       uint64             // position of the cursor
	statement *parser.SelectStmt // the select statement
}

// Transaction represents a transaction
type Transaction struct {
	Statements []*TransactionStmt // Transaction statements
}

// TransactionStmt represents a transaction statement
type TransactionStmt struct {
	Id       int         // The statement id
	Stmt     interface{} // The statement, (insert, update, delete)
	Commited bool        // Whether the statement has been commited
	Rollback *Rollback   // Rollback data
}

// Rollback represents a transaction rollback
type Rollback struct {
	Rows []*Before // Rows to rollback to (if applicable)
}

// Before represents the state of a row before a transaction
type Before struct {
	RowId int64                  // The actual row id within the table
	Row   map[string]interface{} // The row data
}

// Plan represents an execution plan
type Plan struct {
	Steps []*Step // Steps in the plan
}

// Step represents a step in an execution plan
type Step struct {
	Operation EXPLAIN_OP // The operation, whether it be full table scan or index scan
	Table     string     // The table name
	Column    string     // The column name
	IO        int64      // Number of IO operations
}

type EXPLAIN_OP int // When explaining execution we append to explain

const (
	EXPLAIN_SELECT EXPLAIN_OP = iota
	FULL_SCAN
	INDEX_SCAN
)

// New creates a new Executor
// Creates a new AriaSQL executor
// You must pass in a pointer to an AriaSQL instance and a pointer to a Channel instance
// they should be created before calling this function
func New(aria *core.AriaSQL, ch *core.Channel) *Executor {
	return &Executor{ch: ch, aria: aria}
}

// Execute executes an abstract syntax tree statement
func (ex *Executor) Execute(stmt parser.Statement) error {

	// If we are explaining an execution we will create a new plan
	if ex.explaining {
		// Start new plan
		ex.plan = &Plan{}
	}

	// We will handle the statement based on the type
	switch s := stmt.(type) {
	case *parser.BeginStmt:

		// A database must be selected to begin a transaction.  Transactions are of INSERT, UPDATE, DELETE statement type
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "*", []shared.PrivilegeAction{shared.PRIV_COMMIT}) {
			return errors.New("user does not have the privilege to BEGIN a transaction on system. A user must have BEGIN privilege for specific database")
		}

		// Check if transactions already begun
		if ex.TransactionBegun {
			return errors.New("transaction already begun")
		}

		// Set transaction begun flag
		ex.TransactionBegun = true

		ex.Transaction = &Transaction{Statements: []*TransactionStmt{}} // Initialize the transaction

		return nil
	case *parser.RollbackStmt: // Rollback statement

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check user has the privilege to rollback
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "*", []shared.PrivilegeAction{shared.PRIV_ROLLBACK}) {
			return errors.New("user does not have the privilege to ROLLBACK transaction on system. A user must have ROLLBACK privilege for specific database")
		}

		// Check if transaction has begun
		if !ex.TransactionBegun {
			return errors.New("no transaction begun")
		}

		err := ex.rollback() // Rollback the transaction
		if err != nil {
			return err

		}

		return nil
	case *parser.CommitStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check user has the privilege to commit
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "*", []shared.PrivilegeAction{shared.PRIV_COMMIT}) {
			return errors.New("user does not have the privilege to COMMIT transactions on system. A user must have COMMIT privilege for specific database")
		}

		// Transactions are made up of INSERT, UPDATE, DELETE statements
		for j, tx := range ex.Transaction.Statements {
			switch ss := tx.Stmt.(type) {
			case *parser.DeleteStmt: // Execute delete statement

				// Check if a database is selected
				if ex.ch.Database == nil {
					// If somehow nil, rollback the transaction
					err := ex.rollback() // Rollback the transaction
					if err != nil {
						return err

					}
					return errors.New("no database selected")

				}

				// Execute the delete statement
				// Gather deleted rowIds and deleted rows in case of rollback
				rowIds, deletedRows, err := ex.executeDeleteStmt(ss)
				if err != nil {
					// If an error occurs, rollback the transaction
					err = ex.rollback()
					if err != nil {
						return err
					}
				}

				if ex.TransactionBegun { // If transaction has begun

					for i, r := range deletedRows {
						// Append the row to the rollback data
						ex.Transaction.Statements[j].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
							RowId: rowIds[i],
							Row:   r,
						})
					}
				}

				continue
			case *parser.UpdateStmt:

				// Check if a database is selected
				if ex.ch.Database == nil {
					if j > 0 {
						// rollback
						err := ex.rollback()
						if err != nil {
							return err
						}
					}
					return errors.New("no database selected")

				}

				// We get updated rowIds and previous row data in case of rollback
				rowIds, updatedRows, err := ex.executeUpdateStmt(ss)
				if err != nil {
					// If an error occurs, rollback the transaction
					err = ex.rollback()
					if err != nil {
						return err
					}
					return err
				}

				if ex.TransactionBegun {
					// Append the updated rows to the rollback data
					for i, _ := range updatedRows {
						ex.Transaction.Statements[j].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
							RowId: rowIds[i],
							Row:   updatedRows[i],
						})
					}
				}

				continue
			case *parser.InsertStmt:

				// Check if database is nil, cannot be nil
				if ex.ch.Database == nil {
					if j > 0 {
						// rollback
						err := ex.rollback()
						if err != nil {
							return err
						}
					}

					return errors.New("no database selected")
				}

				// Get table for insert
				tbl := ex.ch.Database.GetTable(ss.TableName.Value)
				if tbl == nil {
					if j > 0 {
						// rollback
						err := ex.rollback()
						if err != nil {
							return err
						}
					}
					return errors.New("table does not exist")
				}

				// Check if user has the privilege to insert into the table
				if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, tbl.Name, []shared.PrivilegeAction{shared.PRIV_INSERT}) {
					if j > 0 {
						// rollback
						err := ex.rollback()
						if err != nil {
							return err
						}
					}
					return errors.New("user does not have the privilege to INSERT on system for database " + ex.ch.Database.Name + " and table " + ss.TableName.Value)
				}

				// Rows to be inserted
				var rows []map[string]interface{}

				// Populate new row based on the insert statement
				for _, row := range ss.Values {
					newRow := map[string]interface{}{}
					for i, col := range ss.ColumnNames {
						switch row[i].(type) {
						case *parser.Literal:
							newRow[col.Value] = row[i].(*parser.Literal).Value
						case *shared.GenUUID, *shared.SysDate, *shared.SysTime, *shared.SysTimestamp: // If system function
							newRow[col.Value] = row[i]
						}

					}
					rows = append(rows, newRow)

				}

				// We get inserted rowIds and inserted rows in case of rollback
				rowIds, insertedRows, err := tbl.Insert(rows)
				if err != nil {
					if j > 0 {
						// rollback
						err := ex.rollback()
						if err != nil {
							return err
						}
					}
					return err
				}

				for i, rowId := range rowIds {
					ex.Transaction.Statements[j].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
						RowId: rowId,
						Row:   insertedRows[i],
					})
				}

				continue
			}
		}

		// Transaction has been commited
		ex.TransactionBegun = false // Reset transaction begun flag

		return nil
	case *parser.CreateDatabaseStmt:
		if !ex.recover { // If not recovering from WAL, check if user has the privilege to create a database
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to CREATE on system. A user must have CREATE privilege system wide")
			}
		}

		// Check if a transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Create the database
		return ex.aria.Catalog.CreateDatabase(s.Name.Value)
	case *parser.CreateTableStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to CREATE on system for database " + ex.ch.Database.Name)
			}
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		encKey := s.EncryptKey.Value.(string) // if any

		encKey = strings.TrimSuffix(strings.TrimPrefix(encKey, "'"), "'")

		// Create the table
		err = ex.ch.Database.CreateTable(s.TableName.Value, s.TableSchema, s.Encrypt, s.Compress, []byte(encKey))
		if err != nil {
			return err
		}

		return nil

	case *parser.DropTableStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, s.TableName.Value, []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to DROP on system for database " + ex.ch.Database.Name)
			}
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Drop the table
		err = ex.ch.Database.DropTable(s.TableName.Value)
		if err != nil {
			return err
		}

		return nil
	case *parser.CreateIndexStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to CREATE on system for database " + ex.ch.Database.Name)
			}
		}

		// Get the table
		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		var columns []string // Columns to create index on

		// convert *parser.Identifier to []string
		for _, col := range s.ColumnNames {
			columns = append(columns, col.Value)
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Create the index
		err = tbl.CreateIndex(s.IndexName.Value, columns, s.Unique)
		if err != nil {
			return err
		}

		return nil
	case *parser.DropIndexStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to DROP on system for database " + ex.ch.Database.Name)
			}
		}

		// Get the table
		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Drop the index
		err = tbl.DropIndex(s.IndexName.Value)
		if err != nil {
			return err
		}

		return nil
	case *parser.InsertStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Get table for insertion
		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, s.TableName.Value, []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to INSERT on system for database " + ex.ch.Database.Name + " and table " + s.TableName.Value)
			}
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(stmt))
		if err != nil {
			return err
		}

		// Rows to be inserted
		var rows []map[string]interface{}

		// Populate new row based on the insert statement
		for _, row := range s.Values {
			newRow := map[string]interface{}{}
			for i, col := range s.ColumnNames {
				switch row[i].(type) {
				case *parser.Literal:
					newRow[col.Value] = row[i].(*parser.Literal).Value
				case *shared.GenUUID, *shared.SysDate, *shared.SysTime, *shared.SysTimestamp:
					newRow[col.Value] = row[i]
				}
			}
			rows = append(rows, newRow)

		}

		// Check table schema constraints for check
		for name, colDef := range tbl.TableSchema.ColumnDefinitions {
			if colDef.Check != nil {
				for _, row := range rows {
					r := []map[string]interface{}{row}
					t := []*catalog.Table{tbl}
					var fr []map[string]interface{}

					if !ex.evaluateCondition(colDef.Check, &r, t, &fr) {
						return errors.New("check constraint failed for column " + name)
					}
				}
			}
		}
		if ex.TransactionBegun { // if transaction has begun we append the statement to the transaction
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Id:       len(ex.Transaction.Statements),
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})
		} else {

			_, _, err = tbl.Insert(rows)
			if err != nil {
				return err
			}
		}

		return nil
	case *parser.UseStmt:
		// Get the database
		db := ex.aria.Catalog.GetDatabase(s.DatabaseName.Value)
		if db == nil {
			return errors.New("database does not exist")
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		ex.ch.Database = db // Set current channel database

		return nil
	case *parser.DropDatabaseStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege(stmt.(*parser.DropDatabaseStmt).Name.Value, "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to INSERT on system for database " + stmt.(*parser.DropDatabaseStmt).Name.Value)
			}
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Drop the database
		err = ex.aria.Catalog.DropDatabase(s.Name.Value)
		if err != nil {
			return err
		}

		// if the database is the current database, set the current database to nil
		if ex.ch.Database.Name == s.Name.Value {
			ex.ch.Database = nil
		}

		return nil

	case *parser.SelectStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Execute the select statement
		_, err := ex.executeSelectStmt(s, false)
		if err != nil {
			return err
		}

		return nil
	case *parser.UpdateStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")

		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		if ex.TransactionBegun { // If transaction has begun we append the statement to the transaction
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Id:       len(ex.Transaction.Statements),
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})
		} else {

			_, _, err = ex.executeUpdateStmt(s)
			if err != nil {
				return err
			}
		}

		return nil

	case *parser.DeleteStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")

		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		if ex.TransactionBegun { // If transaction has begun we append the statement to the transaction
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Id:       len(ex.Transaction.Statements),
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})
		} else {

			_, _, err = ex.executeDeleteStmt(s)
			if err != nil {
				return err
			}

		}

		return nil
	case *parser.CreateUserStmt:
		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to CREATE on system")
			}
		}

		// Check if a transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Append the statement to the WAL file
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Create the user
		err = ex.aria.Catalog.CreateNewUser(s.Username.Value, s.Password.Value.(string))
		if err != nil {
			return err
		}

		return nil

	case *parser.DropUserStmt:
		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
				return errors.New("user does not have the privilege to DROP on system")
			}
		}

		// Check if a transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		err = ex.aria.Catalog.DropUser(s.Username.Value)
		if err != nil {
			return err
		}

		return nil

	case *parser.GrantStmt:

		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_GRANT}) {
				return errors.New("user does not have the privilege to GRANT on system")
			}
		}

		// Check if a transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		var priv *catalog.Privilege

		if s.PrivilegeDefinition.Object != nil {

			if len(strings.Split(s.PrivilegeDefinition.Object.Value, ".")) < 2 {
				return errors.New("invalid object ")
			}

			databaseName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[0]
			tableName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[1]

			priv = &catalog.Privilege{
				DatabaseName:     databaseName,
				TableName:        tableName,
				PrivilegeActions: nil,
			}

			for _, action := range s.PrivilegeDefinition.Actions {
				priv.PrivilegeActions = append(priv.PrivilegeActions, action)
			}
		} else {
			priv = &catalog.Privilege{
				DatabaseName:     "*",
				TableName:        "*",
				PrivilegeActions: nil,
			}

			for _, action := range s.PrivilegeDefinition.Actions {
				priv.PrivilegeActions = append(priv.PrivilegeActions, action)
			}
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		err = ex.aria.Catalog.GrantPrivilegeToUser(s.PrivilegeDefinition.Grantee.Value, priv)
		if err != nil {
			return err
		}

		return nil

	case *parser.RevokeStmt:
		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_REVOKE}) {
				return errors.New("user does not have the privilege to REVOKE on system")
			}
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		var priv *catalog.Privilege

		if s.PrivilegeDefinition.Object != nil {

			if len(strings.Split(s.PrivilegeDefinition.Object.Value, ".")) < 2 {
				return errors.New("invalid object ")
			}

			databaseName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[0]
			tableName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[1]

			priv = &catalog.Privilege{
				DatabaseName:     databaseName,
				TableName:        tableName,
				PrivilegeActions: nil,
			}

			for _, action := range s.PrivilegeDefinition.Actions {
				priv.PrivilegeActions = append(priv.PrivilegeActions, action)
			}
		} else {
			priv = &catalog.Privilege{
				DatabaseName:     "*",
				TableName:        "*",
				PrivilegeActions: nil,
			}

			for _, action := range s.PrivilegeDefinition.Actions {
				priv.PrivilegeActions = append(priv.PrivilegeActions, action)
			}
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		err = ex.aria.Catalog.RevokePrivilegeFromUser(s.PrivilegeDefinition.Revokee.Value, priv)
		if err != nil {
			return err
		}

		return nil

	case *parser.ShowStmt:

		if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_SHOW}) {
			return errors.New("user does not have the privilege to SHOW on system") // system wide privilege
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		switch s.ShowType {
		case parser.SHOW_GRANTS:
			users := ex.aria.Catalog.GetUsers()

			results := make([]map[string]interface{}, len(users))

			for i, user := range users {
				u := ex.aria.Catalog.GetUser(user)
				privs := u.GetPrivileges()
				results[i] = map[string]interface{}{"User": user, "Grants": strings.Join(privs, ",")}
			}

			if !ex.json {
				ex.ResultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
			} else {
				var err error
				ex.ResultSetBuffer, err = shared.CreateJSONByteArray(results)
				if err != nil {
					return err
				}
			}
			return nil
		case parser.SHOW_INDEXES:

			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_SHOW}) {
				return errors.New("user does not have the privilege to SHOW on system") // system wide privilege
			}

			if ex.ch.Database == nil {
				return errors.New("no database selected")
			}

			table := ex.ch.Database.GetTable(s.From.Value)
			if table == nil {
				return errors.New("table does not exist")
			}

			indexes := table.GetIndexes()

			results := make([]map[string]interface{}, len(indexes))

			for i, index := range indexes {
				results[i] = map[string]interface{}{"Index": index.Name, "Columns": strings.Join(index.Columns, ","), "Unique": index.Unique}
			}

			return nil
		case parser.SHOW_DATABASES:
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_SHOW}) {
				return errors.New("user does not have the privilege to SHOW on system") // system wide privilege
			}

			databases := ex.aria.Catalog.GetDatabases()
			results := make([]map[string]interface{}, len(databases))

			for i, db := range databases {
				results[i] = map[string]interface{}{"Database": db}
			}

			if !ex.json {
				ex.ResultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
			} else {
				var err error
				ex.ResultSetBuffer, err = shared.CreateJSONByteArray(results)
				if err != nil {
					return err
				}
			}
			return nil
		case parser.SHOW_TABLES:
			if ex.ch.Database == nil {
				return errors.New("no database selected")
			}

			tables := ex.ch.Database.GetTables()
			results := make([]map[string]interface{}, len(tables))

			for i, db := range tables {
				results[i] = map[string]interface{}{"Table": db}
			}

			if !ex.json {
				ex.ResultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
			} else {
				var err error
				ex.ResultSetBuffer, err = shared.CreateJSONByteArray(results)
				if err != nil {
					return err
				}
			}

			return nil

		case parser.SHOW_USERS:

			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_SHOW}) {
				return errors.New("user does not have the privilege to SHOW on system") // system wide privilege
			}

			users := ex.aria.Catalog.GetUsers()

			results := make([]map[string]interface{}, len(users))

			for i, db := range users {
				results[i] = map[string]interface{}{"User": db}
			}

			if !ex.json {
				ex.ResultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
			} else {
				var err error
				ex.ResultSetBuffer, err = shared.CreateJSONByteArray(results)
				if err != nil {
					return err
				}
			}

			return nil
		default:
			return errors.New("unsupported show type")
		}
	case *parser.AlterUserStmt:
		if !ex.recover { // If not recovering from WAL
			if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_ALTER}) {
				return errors.New("user does not have the privilege to ALTER on system") // Altering a user just requires an ALTER privilege system wide
			}
		}

		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		if s.SetType == parser.ALTER_USER_SET_PASSWORD {
			err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
			if err != nil {
				return err
			}

			err = ex.aria.Catalog.AlterUserPassword(s.Username.Value, s.Value.Value.(string))
			if err != nil {
				return err
			}
		} else if s.SetType == parser.ALTER_USER_SET_USERNAME {
			err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
			if err != nil {
				return err
			}

			err = ex.aria.Catalog.AlterUserUsername(s.Username.Value, s.Value.Value.(string))
			if err != nil {
				return err
			}
		} else {
			return errors.New("unsupported set type for alter user")

		}

		return nil
	case *parser.WhileStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Keep executing until error

		for ex.fetchStatus.Load() == int32(s.FetchStatus.Value.(uint64)) {
			for _, cursorStmt := range s.Stmts.Stmts {
				err := ex.Execute(cursorStmt)
				if err != nil {
					return err
				}

			}
		}

		return nil
	case *parser.FetchStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Check if cursors exist

		var err error
		if ex.cursors == nil {
			return errors.New("no cursors")
		}

		if ex.cursors[s.CursorName.Value] == nil {
			return errors.New("cursor does not exist")
		}

		cursor := ex.cursors[s.CursorName.Value]

		// Execute the select statement
		r, err := ex.executeSelectStmt(cursor.statement, true)
		if err != nil {
			return err
		}

		if len(r) <= 0 {
			ex.fetchStatus.Swap(-1)
			for _, col := range s.Into {
				if _, ok := ex.vars[col.Value]; !ok {
					return errors.New("variable not found")
				}

				ex.vars[col.Value].Value = nil
			}
			return nil
		} else {

			for _, row := range r {
				for _, col := range s.Into {
					if _, ok := ex.vars[col.Value]; !ok {
						return errors.New("variable not found")
					}

					switch ex.vars[col.Value].DataType {
					case "INT", "INTEGER", "SMALLINT":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(int)
						break
					case "CHAR", "CHARACTER", "TEXT":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(string)
						break
					case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "DEC":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(float64)
						break
					case "BOOL", "BOOLEAN":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(bool)
						break
					case "DATE":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(time.Time)
						break
					case "DATETIME":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(time.Time)
						break
					case "TIME":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(time.Time)
						break
					case "TIMESTAMP":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].(time.Time)
						break
					case "BINARY", "BLOB":
						ex.vars[col.Value].Value = row[strings.TrimPrefix(col.Value, "@")].([]byte)
						break
					default:
						return errors.New("unsupported data type")

					}

				}
			}

			// Increment the cursor position
			cursor.pos = uint64(cursor.pos + 1)

			cursor.statement.TableExpression.LimitClause.Count = &parser.Literal{Value: uint64(1)}
			cursor.statement.TableExpression.LimitClause.Offset = &parser.Literal{Value: uint64(cursor.pos)}

		}

		return nil

	case *parser.OpenStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Check if cursors exist
		if ex.cursors == nil {
			return errors.New("no cursors")
		}

		if ex.cursors[s.CursorName.Value] == nil {
			return errors.New("cursor does not exist")
		}

		cursor := ex.cursors[s.CursorName.Value]

		if cursor.statement == nil {
			return errors.New("no statement for cursor")
		}

		cursor.statement.TableExpression.LimitClause = &parser.LimitClause{}

		// Add limit and offset to the select statement
		cursor.statement.TableExpression.LimitClause.Count = &parser.Literal{Value: uint64(1)}
		cursor.statement.TableExpression.LimitClause.Offset = &parser.Literal{Value: uint64(cursor.pos)}

		return nil

	case *parser.PrintStmt:
		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		switch s.Expr.(type) {
		case *parser.Literal:
			log.Println(s.Expr.(*parser.Literal).Value) // will print the value of the literal in log
		case *parser.Identifier:
			// is variable call
			if ex.vars == nil {
				return errors.New("variable not found")
			}

			if _, ok := ex.vars[s.Expr.(*parser.Identifier).Value]; !ok {
				return errors.New("variable not found")
			}

			log.Println(ex.vars[s.Expr.(*parser.Identifier).Value].Value) // will print the value of the variable in log

			return nil
		default:
			return errors.New("unsupported print type")
		}
	case *parser.DeclareStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if cursors exist
		if ex.cursors == nil {
			ex.cursors = make(map[string]*Cursor)
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Check if cursor name is not nil
		if s.CursorName != nil {

			if ex.cursors == nil {
				ex.cursors = make(map[string]*Cursor)
			}

			if ex.cursors[s.CursorName.Value] != nil {
				return errors.New("cursor already exists")
			}

			// Check if the cursor has a limit clause
			if s.CursorStmt.TableExpression.LimitClause != nil {
				// Cannot open a cursor with a limit clause
				return errors.New("cannot open a cursor with a limit clause")
			}

			// Check if the cursor has an order by clause
			if s.CursorStmt.TableExpression.OrderByClause != nil {
				// Cannot open a cursor with an order by clause
				return errors.New("cannot open a cursor with an order by clause")
			}

			// Add the cursor to the map
			ex.cursors[s.CursorName.Value] = &Cursor{
				name:      s.CursorName.Value,
				pos:       0,
				statement: s.CursorStmt,
			}

			return nil
		} else if s.CursorVariableName != nil {
			// Check data type
			if s.CursorVariableDataType == nil {
				return errors.New("no data type for variable")
			}

			// Check if variable already exists
			if _, ok := ex.vars[s.CursorVariableName.Value]; ok {
				return errors.New("variable is already allocated")
			}

			// Check if datatype is ok
			if !shared.IsValidDataType(s.CursorVariableDataType.Value) {
				return errors.New("invalid data type for variable")
			}

			if ex.vars == nil {
				ex.vars = make(map[string]*Variable)
			}

			ex.vars[s.CursorVariableName.Value] = &Variable{DataType: s.CursorVariableDataType.Value, Value: nil}

			return nil

		}
	case *parser.CloseStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if cursors exist
		if ex.cursors == nil {
			return errors.New("no cursors")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Check if cursor exists
		if ex.cursors[s.CursorName.Value] == nil {
			return errors.New("cursor does not exist")
		}

		delete(ex.cursors, s.CursorName.Value) // delete the cursor
	case *parser.DeallocateStmt:

		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if cursors exist
		if ex.cursors == nil {
			return errors.New("no cursors")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		if s.CursorName != nil {
			if ex.cursors[s.CursorName.Value] == nil {
				return errors.New("cursor does not exist")
			}

			delete(ex.cursors, s.CursorName.Value) // delete the cursor
		} else if s.CursorVariableName != nil {
			if ex.vars == nil {
				return errors.New("no variables")
			}

			if _, ok := ex.vars[s.CursorVariableName.Value]; !ok {
				return errors.New("variable does not exist")
			}

			delete(ex.vars, s.CursorVariableName.Value) // delete the variable
		}

		return nil
	case *parser.DropProcedureStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Drop the procedure
		err := ex.ch.Database.DropProcedure(s.ProcedureName.Value)
		if err != nil {
			return err
		}

		return nil

	case *parser.CreateProcedureStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Add the procedure to the database
		err := ex.ch.Database.AddProcedure(&catalog.Procedure{
			Name: s.Procedure.Name.Value,
			Proc: s.Procedure,
		})
		if err != nil {
			return err
		}

		return nil

	case *parser.ExecStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		// Write to wal
		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(s))
		if err != nil {
			return err
		}

		// Get the procedure
		proc, err := ex.ch.Database.GetProcedure(s.ProcedureName.Value)
		if err != nil {
			return err
		}

		// Check if the procedure is nil
		if proc == nil {
			return errors.New("procedure does not exist")
		}

		// Add args to vars
		for i, arg := range s.Args {
			if _, ok := ex.vars[proc.Proc.(*parser.Procedure).Parameters[i].Name.Value]; !ok {
				return errors.New("parameter not found")
			}

			switch arg := arg.(type) {
			case *parser.Literal:
				ex.vars[proc.Proc.(*parser.Procedure).Parameters[i].Name.Value].Value = arg.Value
			case *parser.Identifier:
				return errors.New("unsupported argument type")
			}
		}

		// validate parameters
		for _, param := range proc.Proc.(*parser.Procedure).Parameters {
			if _, ok := ex.vars[param.Name.Value]; !ok {
				return errors.New("parameter not found")
			}
		}

		// Execute the procedure
		for _, ss := range proc.Proc.(*parser.Procedure).Body.Stmts {
			err := ex.Execute(ss)
			if err != nil {
				return err
			}

		}

		// Remove the parameters
		for _, param := range proc.Proc.(*parser.Procedure).Parameters {
			delete(ex.vars, param.Name.Value)
		}

		return nil

	case *parser.ExplainStmt:
		// Check if a database is selected
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		// Check if transaction has begun
		if ex.TransactionBegun {
			return errors.New("statement not allowed in a transaction")
		}

		ex.explaining = true // Set explaining flag to true

		// Execute the statement
		err := ex.Execute(s.Stmt)
		if err != nil {
			return err
		}

		ex.explaining = false // Set explaining flag to false

		return nil

	default:
		return errors.New("unsupported statement " + reflect.TypeOf(s).String())

	}

	return errors.New("unsupported statement")
}

// executeSelectStmt executes a select statement
func (ex *Executor) executeSelectStmt(stmt *parser.SelectStmt, subquery bool) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	// Check for select list
	if stmt.SelectList == nil {
		return nil, errors.New("no select list")
	}

	if stmt.SelectList != nil && stmt.TableExpression == nil {
		for i, expr := range stmt.SelectList.Expressions {
			switch expr := expr.Value.(type) {
			case *parser.Literal:
				results = append(results, map[string]interface{}{fmt.Sprintf("%v", expr.Value): expr.Value})
			case *parser.Identifier:
				results = append(results, map[string]interface{}{fmt.Sprintf("%v", expr.Value): expr.Value})
			case *parser.BinaryExpression:
				var val interface{}
				err := evaluateBinaryExpression(expr, &val, nil)
				if err != nil {
					return nil, err
				}

				if stmt.SelectList.Expressions[i].Alias == nil {
					results = append(results, map[string]interface{}{fmt.Sprintf("%v", val): val})
				} else {

					results = append(results, map[string]interface{}{stmt.SelectList.Expressions[i].Alias.Value: val})
				}

			}
		}

	} else if stmt.SelectList != nil && stmt.TableExpression != nil {
		var tbles []*catalog.Table // Table list
		// a table list is the tables required say for a join or not, can be a single table

		// Check if table expression is not nil,
		// if so we need to evaluate the from clause
		// Gathering the proposed tables
		if stmt.TableExpression != nil {
			if stmt.TableExpression.FromClause == nil {
				return nil, errors.New("no from clause") // No from?  We need a from clause, that is the tables for the select
			}
		}

		// Gather tables required for the select, can be 1 or more
		for _, tblExpr := range stmt.TableExpression.FromClause.Tables {

			tbl := ex.ch.Database.GetTable(tblExpr.Name.Value)
			if tbl == nil {
				return nil, errors.New("table does not exist")
			}

			// If there is an alias set the table name temporarily to the alias
			if tblExpr.Alias != nil {
				tbl.Name = tblExpr.Alias.Value
			}

			// Check if user has the privilege to select from the table
			if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, tbl.Name, []shared.PrivilegeAction{shared.PRIV_SELECT}) {
				return nil, errors.New("user does not have the privilege to SELECT on table " + tbl.Name)
			}

			tbles = append(tbles, tbl)
		}

		// Check if there are any tables
		if len(tbles) == 0 {
			return nil, errors.New("no tables")
		} // You can't do this!!  There should be tables

		// search reads tables, the where condition and gathers the rows based on that
		// search will also evaluate joins, subqueries, and other predicates
		// if the column in a predicate is indexed, we can use the index to locate rows faster to evaluate
		rows, err := ex.search(tbles, stmt.TableExpression.WhereClause, nil, false, nil, nil)
		if err != nil {
			return nil, err
		}

		if ex.explaining {
			return nil, nil
		}

		// Pass rows to result set
		results = rows

		//If there is a group by clause
		if stmt.TableExpression.GroupByClause != nil {
			// Group the results
			groupedRows, err := ex.group(results, stmt.TableExpression.GroupByClause)
			if err != nil {
				return nil, err
			}
			// Check for having clause
			if stmt.TableExpression.HavingClause != nil {
				// Filter the results based on the having clause
				results, err = ex.having(groupedRows, stmt.TableExpression.HavingClause)
				if err != nil {
					return nil, err
				}
			} else {
				// No having clause, return the grouped rows
				results = []map[string]interface{}{}
				for _, row := range groupedRows {
					results = append(results, row[0])
				}

			}
		} else {
			// We should evaluate the select list
			// Based on projection (select list), we can filter the columns

			err = ex.selectListFilter(&results, stmt.SelectList)
			if err != nil {
				return nil, err

			}
		}

		// Check for order by
		if stmt.TableExpression.OrderByClause != nil {
			var err error
			results, err = ex.orderBy(results, stmt.TableExpression.OrderByClause)
			if err != nil {
				return nil, err
			}
		}

		// Check for limit and offset
		if stmt.TableExpression.LimitClause != nil {
			offset := 0
			count := len(results)

			if stmt.TableExpression.LimitClause.Offset != nil {
				// Type assertion to uint64
				offset = int(stmt.TableExpression.LimitClause.Offset.Value.(uint64))
			}
			if stmt.TableExpression.LimitClause.Count != nil {
				// Type assertion to uint64
				count = int(stmt.TableExpression.LimitClause.Count.Value.(uint64))
			}

			// Ensure offset and count are within bounds
			if offset > len(results) {
				// If offset is beyond the length of results, return an empty slice
				results = []map[string]interface{}{}
			} else {
				end := offset + count
				if end > len(results) {
					end = len(results) // Adjust end if it exceeds the length of results
				}

				results = results[offset:end]
			}
		}

		// Check for distinct
		if stmt.Distinct {
			results = shared.DistinctMap(results, shared.GetColumns(results)...)
		}

		if stmt.Union != nil {
			// Evaluate the union
			unionResults, err := ex.executeSelectStmt(stmt.Union, true)
			if err != nil {
				return nil, err
			}

			// Merge the results
			results = append(results, unionResults...)

			if !stmt.UnionAll {
				results = shared.DistinctMap(results, shared.GetColumns(results)...)
			}
		}

		if subquery {
			return results, nil
		}

	}

	// Now we format the results
	if !ex.json {
		ex.ResultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
	} else {
		var err error
		ex.ResultSetBuffer, err = shared.CreateJSONByteArray(results)
		if err != nil {
			return nil, err
		}

	}

	return nil, nil // We return rows in result set buffer

}

// executeUpdateStmt updates rows in a table
func (ex *Executor) executeUpdateStmt(stmt *parser.UpdateStmt) ([]int64, []map[string]interface{}, error) {
	var rowIds []int64                // Updated row ids
	var rows []map[string]interface{} // Rows to update
	var updatedRows int
	var tbles []*catalog.Table // Table list

	tbles = append(tbles, ex.ch.Database.GetTable(stmt.TableName.Value))

	// Check if there are any tables
	if len(tbles) == 0 {
		return nil, nil, errors.New("no tables")
	} // You can't do this!!

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	err := ex.filter(stmt.WhereClause, tbles, &rows, &rowIds)
	if err != nil {
		return nil, nil, err
	}

	if ex.explaining {
		return nil, nil, nil
	}

	for i, row := range rows {
		setClause := convertSetClauseToCatalogLike(&stmt.SetClause, &row)

		if i < len(rowIds) {
			if rowIds[i] == 0 {
				err = tbles[0].UpdateRow(rowIds[i], row, setClause)
				if err != nil {
					return nil, nil, err
				}
				updatedRows++
			} else {
				err = tbles[0].UpdateRow(rowIds[i]-1, row, setClause)
				if err != nil {
					return nil, nil, err
				}
				updatedRows++
			}
		}
	}

	rowsAffected := map[string]interface{}{"RowsAffected": updatedRows}
	rows = []map[string]interface{}{rowsAffected}

	// Now we format the results
	if !ex.json {
		ex.ResultSetBuffer = shared.CreateTableByteArray(rows, shared.GetHeaders(rows))
	} else {
		var err error
		ex.ResultSetBuffer, err = shared.CreateJSONByteArray(rows)
		if err != nil {
			return nil, nil, err
		}

	}

	return nil, nil, nil

}

// executeDeleteStmt deletes rows from a table
func (ex *Executor) executeDeleteStmt(stmt *parser.DeleteStmt) ([]int64, []map[string]interface{}, error) {
	var rowIds []int64                // Updated row ids
	var rows []map[string]interface{} // Rows before deletion
	var deletedRows int
	var tbles []*catalog.Table // Table list

	tbles = append(tbles, ex.ch.Database.GetTable(stmt.TableName.Value))

	// Check if there are any tables
	if len(tbles) == 0 {
		return nil, nil, errors.New("no tables")
	} // You can't do this!!

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	err := ex.filter(stmt.WhereClause, tbles, &rows, &rowIds)
	if err != nil {
		return nil, nil, err
	}

	if ex.explaining {
		return nil, nil, nil
	}

	for i := range rows {
		err = tbles[0].DeleteRow(rowIds[i] - 1)
		if err != nil {
			return nil, nil, err
		}
		deletedRows++

	}

	rowsAffected := map[string]interface{}{"RowsAffected": deletedRows}
	rows = []map[string]interface{}{rowsAffected}

	// Now we format the results
	if !ex.json {
		ex.ResultSetBuffer = shared.CreateTableByteArray(rows, shared.GetHeaders(rows))
	} else {
		var err error
		ex.ResultSetBuffer, err = shared.CreateJSONByteArray(rows)
		if err != nil {
			return nil, nil, err
		}

	}

	return rowIds, rows, nil

}

// convertSetClauseToCatalogLike converts a set clause(s) to a catalog set clause(s)
func convertSetClauseToCatalogLike(setClause *[]*parser.SetClause, row *map[string]interface{}) []*catalog.SetClause {
	var setClauses []*catalog.SetClause

	for _, set := range *setClause {
		var val interface{}

		// check if set.Value.Value is *BinaryExpression
		// if so, evaluate the expression
		if _, ok := set.Value.Value.(*parser.BinaryExpression); ok {
			err := evaluateBinaryExpression(set.Value.Value.(*parser.BinaryExpression), &val, &[]map[string]interface{}{*row})
			if err != nil {
				return nil
			}

		} else {
			val = set.Value.Value
		}

		setClauses = append(setClauses, &catalog.SetClause{
			ColumnName: set.Column.Value,
			Value:      val,
		})

	}

	return setClauses

}

// having filters the results based on the having clause
func (ex *Executor) having(groupedRows map[interface{}][]map[string]interface{}, having *parser.HavingClause) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for _, row := range groupedRows {
		switch having.SearchCondition.(type) {
		case *parser.LogicalCondition:
			// left and right must be comparison predicates

			// we will recursively evaluate the left and right conditions

			leftHaving := having.SearchCondition.(*parser.LogicalCondition).Left.(*parser.ComparisonPredicate)
			rightHaving := having.SearchCondition.(*parser.LogicalCondition).Right.(*parser.ComparisonPredicate)

			i, err := ex.having(groupedRows, &parser.HavingClause{SearchCondition: leftHaving})
			if err != nil {
				return nil, err
			}

			j, err := ex.having(groupedRows, &parser.HavingClause{SearchCondition: rightHaving})
			if err != nil {
				return nil, err
			}

			switch having.SearchCondition.(*parser.LogicalCondition).Op {
			case parser.OP_AND:
				if len(i) > 0 && len(j) > 0 {
					results = append(results, row[0])
				}
			case parser.OP_OR:
				if len(i) > 0 || len(j) > 0 {
					results = append(results, row[0])
				}
			}

		case *parser.ComparisonPredicate:
			// left must be an aggregate function
			// right must be a literal

			// Get the aggregate function
			aggFunc := having.SearchCondition.(*parser.ComparisonPredicate).Left.Value.(*parser.AggregateFunc)

			// Get the right value
			//rightVal := having.SearchCondition.(*parser.ComparisonPredicate).Right.Value.(*parser.Literal).Value

			// Get the aggregate function name
			aggFuncName := aggFunc.FuncName

			// Get the aggregate function arguments
			aggFuncArgs := aggFunc.Args

			// Get the column name
			//colName := aggFuncArgs[0].(*parser.ColumnSpecification).ColumnName.Value

			switch aggFuncName {
			case "COUNT":
				count := len(row)

				having.SearchCondition.(*parser.ComparisonPredicate).Left.Value = &parser.Literal{Value: count}

				rows := []map[string]interface{}{
					{"COUNT": count},
				}
				ok := ex.evaluateCondition(having.SearchCondition, &rows, nil, nil)
				if ok {
					results = append(results, row[0])
				}

			case "SUM":
				// Sum the values
				var sum int

				for _, r := range row {
					for _, arg := range aggFuncArgs {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := r[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
							}

							switch r[arg.ColumnName.Value].(type) {
							case int:
								sum += r[arg.ColumnName.Value].(int)
							case int64:
								sum += int(r[arg.ColumnName.Value].(int64))
							case float64:
								sum += int(r[arg.ColumnName.Value].(float64))

							}
						}

					}
				}

				newComparisonPredicate := parser.ComparisonPredicate{
					Left: &parser.ValueExpression{Value: &parser.Literal{Value: sum}},
				}

				rows := []map[string]interface{}{
					{aggFuncArgs[0].(*parser.ColumnSpecification).ColumnName.Value: sum},
				}

				ok := ex.evaluateCondition(newComparisonPredicate, &rows, nil, nil)
				if ok {
					results = append(results, row[0])
				}

			case "AVG":
				// Average the values
				var sum int
				var count int

				for _, r := range row {
					for _, arg := range aggFuncArgs {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := r[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
							}

							switch r[arg.ColumnName.Value].(type) {
							case int:
								sum += r[arg.ColumnName.Value].(int)
							case int64:
								sum += int(r[arg.ColumnName.Value].(int64))
							case float64:
								sum += int(r[arg.ColumnName.Value].(float64))

							}
						}

					}
				}

				count = len(row)
				avg := sum / count

				rows := []map[string]interface{}{
					{"AVG": avg},
				}
				ok := ex.evaluateCondition(having.SearchCondition, &rows, nil, nil)
				if ok {
					results = append(results, row[0])
				}

			case "MAX":
				// Find the maximum value

				var mx int

				for _, r := range row {
					for _, arg := range aggFuncArgs {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := r[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
							}

							switch r[arg.ColumnName.Value].(type) {
							case int:
								if r[arg.ColumnName.Value].(int) > mx {
									mx = r[arg.ColumnName.Value].(int)
								}
							case int64:
								if int(r[arg.ColumnName.Value].(int64)) > mx {
									mx = int(r[arg.ColumnName.Value].(int64))
								}
							case float64:
								if int(r[arg.ColumnName.Value].(float64)) > mx {
									mx = int(r[arg.ColumnName.Value].(float64))
								}
							}
						}
					}
				}

				rows := []map[string]interface{}{
					{"MIN": mx},
				}
				ok := ex.evaluateCondition(having.SearchCondition, &rows, nil, nil)
				if ok {
					results = append(results, row[0])
				}

			case "MIN":
				// Find the minimum value

				var mn int

				mn = int(^uint(0) >> 1)

				for _, r := range row {
					for _, arg := range aggFuncArgs {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := r[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
							}

							switch r[arg.ColumnName.Value].(type) {
							case int:
								if r[arg.ColumnName.Value].(int) < mn {
									mn = r[arg.ColumnName.Value].(int)
								}
							case int64:
								if int(r[arg.ColumnName.Value].(int64)) < mn {
									mn = int(r[arg.ColumnName.Value].(int64))
								}
							case float64:
								if int(r[arg.ColumnName.Value].(float64)) < mn {
									mn = int(r[arg.ColumnName.Value].(float64))
								}
							}
						}
					}
				}

				rows := []map[string]interface{}{
					{"MIN": mn},
				}
				ok := ex.evaluateCondition(having.SearchCondition, &rows, nil, nil)
				if ok {
					results = append(results, row[0])
				}
			}

		}
	}

	return results, nil
}

// group groups the results
func (ex *Executor) group(results []map[string]interface{}, groupBy *parser.GroupByClause) (map[interface{}][]map[string]interface{}, error) {

	grouped := make(map[interface{}][]map[string]interface{})
	if groupBy == nil {
		return grouped, nil
	}

	if len(groupBy.GroupByExpressions) == 0 {
		return grouped, nil
	}

	// Iterate through the data
	for _, entry := range results {
		// Get the group key value
		groupValue := entry[groupBy.GroupByExpressions[0].Value.(*parser.ColumnSpecification).ColumnName.Value]

		// Append the entry to the slice corresponding to the group key value
		grouped[groupValue] = append(grouped[groupValue], entry)
	}

	return grouped, nil
}

// getFirstAggFuncFromBinaryExpression gets the first aggregate function from a binary expression
func getFirstAggFuncFromBinaryExpression(expr *parser.BinaryExpression) *parser.AggregateFunc {
	switch expr.Left.(type) {
	case *parser.AggregateFunc:
		return expr.Left.(*parser.AggregateFunc)
	case *parser.BinaryExpression:
		return getFirstAggFuncFromBinaryExpression(expr.Left.(*parser.BinaryExpression))
	}

	return nil

}

// selectListFilter filters the results based on the select list
func (ex *Executor) selectListFilter(results *[]map[string]interface{}, selectList *parser.SelectList) error {

	if ex.explaining {
		return nil

	}

	if selectList == nil {
		return errors.New("no select list")
	}

	if len(selectList.Expressions) == 0 {
		return errors.New("no select list")
	}

	var columns []string // The columns to be selected

	for i, expr := range selectList.Expressions {

		switch expr := expr.Value.(type) {

		case *parser.BinaryExpression:

			// Left should be a column

			col := getFirstLeftBinaryExpressionColumn(expr)

			if col != nil {

				for j, row := range *results {
					var val interface{}

					err := evaluateBinaryExpression(expr, &val, &[]map[string]interface{}{row})
					if err != nil {
						return err
					}

					if selectList.Expressions[i].Alias == nil {

						// update corresponding column
						(*results)[j][col.ColumnName.Value] = val
					} else {
						// update corresponding column
						(*results)[j][selectList.Expressions[i].Alias.Value] = val
						// delete the old column
						delete((*results)[j], col.ColumnName.Value)
					}

				}

				if selectList.Expressions[i].Alias == nil {
					columns = append(columns, col.ColumnName.Value)
				} else {
					columns = append(columns, selectList.Expressions[i].Alias.Value)
				}
			} else {
				// is aggregate function
				// evaluate the expression
				var val interface{}

				err := evaluateBinaryExpression(expr, &val, results)
				if err != nil {
					return err
				}

				if selectList.Expressions[i].Alias == nil {
					*results = []map[string]interface{}{map[string]interface{}{getFirstAggFuncFromBinaryExpression(expr).FuncName: val}}
					columns = []string{getFirstAggFuncFromBinaryExpression(expr).FuncName}
				} else {
					*results = []map[string]interface{}{map[string]interface{}{selectList.Expressions[i].Alias.Value: val}}
					columns = []string{selectList.Expressions[i].Alias.Value}
				}

			}

		case *parser.Wildcard:

			return nil
		case *parser.ColumnSpecification:

			// Check for alias
			if selectList.Expressions[i].Alias == nil {
				if expr.ColumnName.Value == "*" && expr.TableName != nil {
					for _, row := range *results {
						for k, v := range row {
							if strings.HasPrefix(k, expr.TableName.Value+".") {
								row[k] = v
								columns = append(columns, k)
							}
						}
					}

				}

			} else {
				columns = append(columns, selectList.Expressions[i].Alias.Value)
				// Replace all instances of the column name with the alias
				for _, row := range *results {

					if _, ok := row[expr.ColumnName.Value]; ok {
						row[selectList.Expressions[i].Alias.Value] = row[expr.ColumnName.Value]
						delete(row, expr.ColumnName.Value)
					}
				}
			}

			columns = append(columns, expr.ColumnName.Value)
		case *parser.AggregateFunc:
			var err error

			// evaluate the aggregate function
			err = evaluateAggregate(expr, results, &columns, selectList.Expressions[i].Alias)
			if err != nil {
				return err
			}
		case *parser.CaseExpr:
			var err error

			err = ex.evaluateSelectCase(expr, results, &columns, selectList.Expressions[i].Alias)
			if err != nil {
				return err
			}
		case *parser.UpperFunc, *parser.LowerFunc, *parser.LengthFunc, *parser.PositionFunc, *parser.RoundFunc,
			*parser.TrimFunc, *parser.SubstrFunc, *parser.ConcatFunc, *parser.CastFunc, *shared.GenUUID, *shared.SysDate,
			*shared.SysTime, *shared.SysTimestamp, *parser.CoalesceFunc, *parser.ReverseFunc:
			var err error
			err = evaluateSystemFunc(expr, results, &columns, selectList.Expressions[i].Alias)
			if err != nil {
				return err
			}
		}

	}

	for _, row := range *results {
		for k, _ := range row {
			if !slices.Contains(columns, k) {
				delete(row, k)
			}
		}

	}

	return nil

}

// evaluateSelectCase evaluates a case expression within a select list
func (ex *Executor) evaluateSelectCase(expr interface{}, results *[]map[string]interface{}, columns *[]string, alias *parser.Identifier) error {
	switch expr := expr.(type) {
	case *parser.CaseExpr:
		for i, _ := range *results {

			if len(expr.WhenClauses) == 0 {
				return errors.New("no when clauses in case expression")
			}

			for _, when := range expr.WhenClauses {

				ok := ex.evaluateCondition(when.Condition, results, nil, nil)

				if ok {
					var result interface{}

					switch when.Result.(*parser.ValueExpression).Value.(type) {
					case *parser.ColumnSpecification:
						result = (*results)[i][when.Result.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value]
					case *parser.Literal:
						result = when.Result.(*parser.ValueExpression).Value.(*parser.Literal).Value
					case *parser.BinaryExpression:
						err := evaluateBinaryExpression(when.Result.(*parser.ValueExpression).Value.(*parser.BinaryExpression), &result, results)
						if err != nil {
							return err
						}

					}

					if alias == nil {
						(*results)[i]["case_result"] = result
						*columns = append(*columns, "case_result")
					} else {
						(*results)[i][alias.Value] = result
						*columns = append(*columns, alias.Value)
					}
				}
			}

			// in the CASE of CASE ;)
			// We use the furthest left column, thats the column we are evaluating and updating
			// We can find that in the first when condition

		}
	}

	return nil
}

// evaluateSystemFunc evaluates system functions like UPPER within a select list
func evaluateSystemFunc(expr interface{}, results *[]map[string]interface{}, columns *[]string, alias *parser.Identifier) error {
	switch expr := expr.(type) {
	case *parser.ConcatFunc:
		for i, row := range *results {
			for k, v := range row {

				if _, ok := row[k].(string); ok {
					newStr := ""

					replaceK := false

					for _, arg := range expr.Args {
						switch arg := arg.(type) {
						case *parser.ValueExpression:
							switch arg.Value.(type) {
							case *parser.ColumnSpecification:
								if arg.Value.(*parser.ColumnSpecification).ColumnName.Value == k {
									newStr += strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")
									replaceK = true

								}
							case *parser.Literal:
								newStr += strings.TrimSuffix(strings.TrimPrefix(arg.Value.(*parser.Literal).Value.(string), "'"), "'")

							}
						}

					}

					if replaceK {

						if alias == nil {
							(*results)[i][k] = fmt.Sprintf("'%s'", newStr)
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = fmt.Sprintf("'%s'", newStr)
							*columns = append(*columns, alias.Value)
						}
					}

				}
			}

		}
	case *parser.SubstrFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {
						startPos := int(expr.StartPos.Value.(uint64))
						endPos := int(expr.Length.Value.(uint64))

						if len(v.(string)) < startPos {
							return errors.New("start position is greater than the length of the string")
						}

						if len(v.(string)) < endPos {
							return errors.New("end position is greater than the length of the string")
						}

						if alias == nil {
							(*results)[i][k] = fmt.Sprintf("'%s'", strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")[startPos-1:endPos])
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = fmt.Sprintf("'%s'", strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")[startPos-1:endPos])
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.TrimFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {
						if alias == nil {
							(*results)[i][k] = fmt.Sprintf("'%s'", strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = fmt.Sprintf("'%s'", strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.LengthFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {
						if alias == nil {
							(*results)[i][k] = len(strings.TrimPrefix(strings.TrimSuffix(v.(string), "'"), "'"))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = len(strings.TrimPrefix(strings.TrimSuffix(v.(string), "'"), "'"))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.PositionFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {
						if alias == nil {
							(*results)[i][k] = strings.Index(v.(string), strings.TrimSuffix(strings.TrimPrefix(expr.In.(*parser.ValueExpression).Value.(*parser.Literal).Value.(string), "'"), "'"))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = strings.Index(v.(string), strings.TrimSuffix(strings.TrimPrefix(expr.In.(*parser.ValueExpression).Value.(*parser.Literal).Value.(string), "'"), "'"))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.RoundFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(float64); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {

						if alias == nil {

							(*results)[i][k] = math.Round(v.(float64))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = math.Round(v.(float64))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.ReverseFunc:

		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {

						if alias == nil {

							(*results)[i][k] = shared.ReverseString(v.(string))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = shared.ReverseString(v.(string))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}

		}
	case *parser.CoalesceFunc:
		for i, row := range *results {
			for k, _ := range row {
				if _, ok := row[k]; ok {

					for _, arg := range expr.Args {
						if arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {
							if alias == nil {
								(*results)[i][k] = expr.Value.(*parser.ValueExpression).Value.(*parser.Literal).Value
								*columns = append(*columns, k)
							} else {
								(*results)[i][alias.Value] = expr.Value.(*parser.ValueExpression).Value.(*parser.Literal).Value
								*columns = append(*columns, alias.Value)
							}
						}
					}
				}
			}
		}

	case *parser.UpperFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {

						if alias == nil {

							(*results)[i][k] = strings.ToUpper(v.(string))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = strings.ToUpper(v.(string))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}
		}
	case *parser.LowerFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {

						if alias == nil {

							(*results)[i][k] = strings.ToLower(v.(string))
							*columns = append(*columns, k)
						} else {
							(*results)[i][alias.Value] = strings.ToLower(v.(string))
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}
		}
	case *parser.CastFunc:
		for i, row := range *results {
			for k, v := range row {
				if _, ok := row[k].(string); ok {
					if expr.Expr.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value == k {

						if alias == nil {

							switch expr.DataType.Value {
							case "INT", "INTEGER", "SMALLINT":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to int
									intVal, err := strconv.Atoi(v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][k] = intVal

								}
							case "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to float
									floatVal, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'"), 64)
									if err != nil {
										return nil
									}

									(*results)[i][k] = floatVal

								}
							case "DATE":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("2006-01-02", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][k] = dateVal

								}
							case "DATETIME", "TIMESTAMP":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("2006-01-02 15:04:05", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][k] = dateVal

								}
							case "TIME":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("15:04:05", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][k] = dateVal

								}
							case "BOOL", "BOOLEAN":
								// check if row value is string
								if _, ok := v.(string); ok {
									if v.(string) == "T" || v.(string) == "t" {
										v = "true"
									} else if v.(string) == "F" || v.(string) == "f" {
										v = "false"
									} else if v.(string) == "'true'" {
										v = "true"
									} else if v.(string) == "'false'" {
										v = "false"
									}

									// Convert string to bool
									boolVal, err := strconv.ParseBool(v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][k] = boolVal

								}
							}

							*columns = append(*columns, k)
						} else {
							switch expr.DataType.Value {
							case "INT", "INTEGER", "SMALLINT":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to int
									intVal, err := strconv.Atoi(v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = intVal

								}
							case "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to float
									floatVal, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'"), 64)
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = floatVal

								}
							case "DATE":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("2006-01-02", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = dateVal

								}
							case "DATETIME", "TIMESTAMP":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("2006-01-02 15:04:05", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = dateVal

								}
							case "TIME":
								// check if row value is string
								if _, ok := v.(string); ok {
									// Convert string to time.Time
									dateVal, err := time.Parse("15:04:05", v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = dateVal

								}
							case "BOOL", "BOOLEAN":
								// check if row value is string
								if _, ok := v.(string); ok {
									if v.(string) == "T" || v.(string) == "t" {
										v = "true"
									} else if v.(string) == "F" || v.(string) == "f" {
										v = "false"
									} else if v.(string) == "'true'" {
										v = "true"
									} else if v.(string) == "'false'" {
										v = "false"
									}

									// Convert string to bool
									boolVal, err := strconv.ParseBool(v.(string))
									if err != nil {
										return nil
									}

									(*results)[i][alias.Value] = boolVal

								}
							}
							*columns = append(*columns, alias.Value)
						}
					}
				}
			}
		}
	}

	return nil
}

// evaluateAggregate evaluates an aggregate function
func evaluateAggregate(expr *parser.AggregateFunc, results *[]map[string]interface{}, columns *[]string, alias *parser.Identifier) error {
	switch expr.FuncName {
	case "COUNT":
		count := 0

		for _, row := range *results {
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *parser.ColumnSpecification:
					if _, ok := row[arg.ColumnName.Value]; !ok {
						return errors.New("column does not exist")
					}
					count++
				case *parser.Wildcard:
					count++
				}
			}
		}

		if alias == nil {
			*results = []map[string]interface{}{map[string]interface{}{"COUNT": count}}
			*columns = []string{"COUNT"}
		} else {
			*results = []map[string]interface{}{map[string]interface{}{alias.Value: count}}
			*columns = []string{alias.Value}
		}

	case "SUM":
		// Sum the values
		var sum int

		for _, row := range *results {
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *parser.ColumnSpecification:

					if _, ok := row[arg.ColumnName.Value]; !ok {
						return errors.New("column does not exist ")
					}

					switch row[arg.ColumnName.Value].(type) {
					case int:
						sum += row[arg.ColumnName.Value].(int)
					case int64:
						sum += int(row[arg.ColumnName.Value].(int64))
					case float64:
						sum += int(row[arg.ColumnName.Value].(float64))

					}
				}

			}

		}

		if alias == nil {
			*results = []map[string]interface{}{map[string]interface{}{"SUM": sum}}
			*columns = []string{"SUM"}
		} else {
			*results = []map[string]interface{}{map[string]interface{}{alias.Value: sum}}
			*columns = []string{alias.Value}
		}

	case "AVG":
		// Average the values
		var sum int
		var count int

		for _, row := range *results {
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *parser.ColumnSpecification:
					if _, ok := row[arg.ColumnName.Value]; !ok {
						return errors.New("column does not exist")
					}

					switch row[arg.ColumnName.Value].(type) {
					case int:
						sum += row[arg.ColumnName.Value].(int)
					case int64:
						sum += int(row[arg.ColumnName.Value].(int64))
					case float64:
						sum += int(row[arg.ColumnName.Value].(float64))

					}
				}

			}
		}

		count = len(*results)

		avg := sum / count

		if alias == nil {
			*results = []map[string]interface{}{map[string]interface{}{"AVG": avg}}
			*columns = []string{"AVG"}
		} else {
			*results = []map[string]interface{}{map[string]interface{}{alias.Value: avg}}
			*columns = []string{alias.Value}
		}

	case "MAX":
		// Find the maximum value
		var mx int

		for _, row := range *results {
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *parser.ColumnSpecification:
					if _, ok := row[arg.ColumnName.Value]; !ok {
						return errors.New("column does not exist")
					}

					switch row[arg.ColumnName.Value].(type) {
					case int:
						if row[arg.ColumnName.Value].(int) > mx {
							mx = row[arg.ColumnName.Value].(int)
						}
					case int64:
						if int(row[arg.ColumnName.Value].(int64)) > mx {
							mx = int(row[arg.ColumnName.Value].(int64))
						}
					case float64:
						if int(row[arg.ColumnName.Value].(float64)) > mx {
							mx = int(row[arg.ColumnName.Value].(float64))
						}
					}
				}
			}
		}

		if alias == nil {
			*results = []map[string]interface{}{map[string]interface{}{"MAX": mx}}
			*columns = []string{"MAX"}
		} else {
			*results = []map[string]interface{}{map[string]interface{}{alias.Value: mx}}
			*columns = []string{alias.Value}
		}

	case "MIN":
		// Find the minimum value
		var mn int
		mn = int(^uint(0) >> 1)

		for _, row := range *results {
			for _, arg := range expr.Args {
				switch arg := arg.(type) {
				case *parser.ColumnSpecification:
					if _, ok := row[arg.ColumnName.Value]; !ok {
						return errors.New("column does not exist")
					}

					switch row[arg.ColumnName.Value].(type) {
					case int:
						if row[arg.ColumnName.Value].(int) < mn {
							mn = row[arg.ColumnName.Value].(int)
						}
					case int64:
						if int(row[arg.ColumnName.Value].(int64)) < mn {
							mn = int(row[arg.ColumnName.Value].(int64))
						}
					case float64:
						if int(row[arg.ColumnName.Value].(float64)) < mn {
							mn = int(row[arg.ColumnName.Value].(float64))
						}
					}
				}
			}
		}

		if alias == nil {
			*results = []map[string]interface{}{map[string]interface{}{"MIN": mn}}
			*columns = []string{"MIN"}
		} else {
			*results = []map[string]interface{}{map[string]interface{}{alias.Value: mn}}
			*columns = []string{alias.Value}
		}
	}

	return nil
}

// search searches tables based on the where clause
func (ex *Executor) search(tbls []*catalog.Table, where *parser.WhereClause, update *[]*parser.SetClause, del bool, rowIds *[]int64, before *[]map[string]interface{}) ([]map[string]interface{}, error) {
	var filteredRows []map[string]interface{} // The final rows that are filtered based on the where clause

	// Check if there are no tables
	if len(tbls) == 0 {
		return nil, errors.New("no tables")
	}

	// Check if there is no where clause
	if where == nil {

		// If there is no where clause, we return all rows from whatever tables were passed
		for _, tbl := range tbls {

			// If we are explaining, we add a full scan step to the plan
			if ex.explaining {
				ex.plan.Steps = append(ex.plan.Steps, &Step{Operation: FULL_SCAN, Table: tbl.Name, Column: "n/a", IO: tbl.IOCount()})
				continue
			}

			// Setup new row iterator
			iter := tbl.NewIterator()

			for iter.Valid() {
				// For every row in the table, we append it to the filtered rows
				row, err := iter.Next()
				if err != nil {
					continue
				}

				filteredRows = append(filteredRows, row)
			}
		}

		if ex.explaining {
			ex.ResultSetBuffer = shared.CreateTableByteArray(convertPlanToRows(ex.plan), shared.GetHeaders(convertPlanToRows(ex.plan)))
			return filteredRows, nil
		}

		for i, row := range filteredRows {
			for k, v := range row {
				// Check if value is time.Time
				if _, ok := v.(time.Time); ok {

					tbl := tbls[0]

					// get the column type and format if need be
					for name, col := range tbl.TableSchema.ColumnDefinitions {

						if name == k {
							switch col.DataType {
							case "DATE":
								filteredRows[i][k] = v.(time.Time).Format("2006-01-02")
							case "TIME":
								filteredRows[i][k] = v.(time.Time).Format("15:04:05")
							case "TIMESTAMP":
								filteredRows[i][k] = v.(time.Time).Format("2006-01-02 15:04:05")
							case "DATETIME":
								filteredRows[i][k] = v.(time.Time).Format("2006-01-02 15:04:05")
							}
						}
					}

				}

			}
		}

	} else {

		var err error

		// where is the where clause
		// filteredRows is the final rows from this function that are filtered based on the where clause
		// tbls are the tables that are being filtered
		// update is a set of set clauses for an update statement, if this is an update statement
		// del is a flag to indicate if this is a delete statement
		// before is a list of rows before a delete or update statement

		err = ex.filter(where, tbls, &filteredRows, rowIds)
		if err != nil {
			return nil, err

		}

	}

	return filteredRows, nil

}

// Optimize struct
// Reads abstract syntax tree and collections tables and columns to check for index optimization
type Optimize struct {
	Tables map[string][]map[string]interface{} // Table and columns to check
}

// opt optimizes the where clause
func (ex *Executor) opt(cond interface{}, optimize *Optimize, tbls []*catalog.Table) error {
	switch cond.(type) {
	case *parser.LogicalCondition:
		err := ex.opt(cond.(*parser.LogicalCondition).Left, optimize, tbls)
		if err != nil {
			return err

		}

		err = ex.opt(cond.(*parser.LogicalCondition).Right, optimize, tbls)
		if err != nil {
			return err
		}

	case *parser.ComparisonPredicate:
		// check if left is column spec
		if _, ok := cond.(*parser.ComparisonPredicate).Left.Value.(*parser.ColumnSpecification); ok {
			col := cond.(*parser.ComparisonPredicate).Left.Value.(*parser.ColumnSpecification)

			var tbl *catalog.Table

			// In case of aliases
			if col.TableName != nil {
				for _, t := range tbls {
					if t.Name == col.TableName.Value {
						tbl = t
						break
					}
				}
			}

			if tbl == nil {
				// Get first table in tables list
				tbl = ex.ch.Database.GetTable(tbls[0].Name)
				if tbl == nil {
					return errors.New("table does not exist")
				}

				col.TableName = &parser.Identifier{Value: tbl.Name}
			}

			iter := tbl.NewIterator()
			if iter.Valid() {
				row, err := iter.Next()
				if err != nil {
					return err
				}

				for k, _ := range row {
					if k == col.ColumnName.Value {

						optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": row[k]})

						// check if right is column spec
						if _, ok := cond.(*parser.ComparisonPredicate).Right.Value.(*parser.ColumnSpecification); ok {

							col = cond.(*parser.ComparisonPredicate).Right.Value.(*parser.ColumnSpecification)

							iter := tbl.NewIterator()
							if iter.Valid() {
								row, err := iter.Next()
								if err != nil {
									return err
								}

								for k, _ := range row {
									if k == col.ColumnName.Value {
										optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": row[k]})

										break // break out of loop
									}
								}
							}

							break // break out of loop
						}

						break // break out of loop
					}
				}
			}

		}

	case *parser.InPredicate:
		// check if left is column spec
		if _, ok := cond.(*parser.InPredicate).Left.Value.(*parser.ColumnSpecification); ok {
			col := cond.(*parser.InPredicate).Left.Value.(*parser.ColumnSpecification)

			if col.TableName == nil {

				// Get first table in tables list
				tbl := ex.ch.Database.GetTable(tbls[0].Name)
				if tbl == nil {
					return errors.New("table does not exist")
				}

				iter := tbl.NewIterator()
				if iter.Valid() {
					row, err := iter.Next()
					if err != nil {
						return err
					}

					for k, _ := range row {
						if k == col.ColumnName.Value {
							col.TableName = &parser.Identifier{Value: tbl.Name}
							break // break out of loop
						}
					}

				}
			}

			for _, val := range cond.(*parser.InPredicate).Values {
				optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": val.Value})
			}

		}
	case *parser.BetweenPredicate:
		// check if left is column spec
		if _, ok := cond.(*parser.BetweenPredicate).Left.Value.(*parser.ColumnSpecification); ok {
			col := cond.(*parser.BetweenPredicate).Left.Value.(*parser.ColumnSpecification)

			if col.TableName == nil {

				// Get first table in tables list
				tbl := ex.ch.Database.GetTable(tbls[0].Name)
				if tbl == nil {
					return errors.New("table does not exist")
				}

				iter := tbl.NewIterator()
				if iter.Valid() {
					row, err := iter.Next()
					if err != nil {
						return err
					}

					for k, _ := range row {
						if k == col.ColumnName.Value {
							col.TableName = &parser.Identifier{Value: tbl.Name}
							break // break out of loop
						}
					}

				}
			}

			if _, ok := optimize.Tables[col.TableName.Value]; !ok {
				optimize.Tables[col.TableName.Value] = []map[string]interface{}{}
			}

			optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": cond.(*parser.BetweenPredicate).Upper.Value})
			optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": cond.(*parser.BetweenPredicate).Lower.Value})

		}
	case *parser.LikePredicate:
		// check if left is column spec
		if _, ok := cond.(*parser.LikePredicate).Left.Value.(*parser.ColumnSpecification); ok {

			col := cond.(*parser.LikePredicate).Left.Value.(*parser.ColumnSpecification)

			if col.TableName == nil {

				// Get first table in tables list
				tbl := ex.ch.Database.GetTable(tbls[0].Name)
				if tbl == nil {
					return errors.New("table does not exist")
				}

				iter := tbl.NewIterator()
				if iter.Valid() {
					row, err := iter.Next()
					if err != nil {
						return err
					}

					for k, _ := range row {
						if k == col.ColumnName.Value {
							col.TableName = &parser.Identifier{Value: tbl.Name}
							break // break out of loop
						}
					}

				}
			}

			if _, ok := optimize.Tables[col.TableName.Value]; !ok {
				optimize.Tables[col.TableName.Value] = []map[string]interface{}{}
			}

			optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": cond.(*parser.LikePredicate).Pattern.Value})

		}
	case *parser.IsPredicate:
		// check if left is column spec
		if _, ok := cond.(*parser.IsPredicate).Left.Value.(*parser.ColumnSpecification); ok {
			col := cond.(*parser.IsPredicate).Left.Value.(*parser.ColumnSpecification)

			if col.TableName == nil {

				// Get first table in tables list
				tbl := ex.ch.Database.GetTable(tbls[0].Name)
				if tbl == nil {
					return errors.New("table does not exist")
				}

				iter := tbl.NewIterator()
				if iter.Valid() {
					row, err := iter.Next()
					if err != nil {
						return err
					}

					for k, _ := range row {
						if k == col.ColumnName.Value {
							col.TableName = &parser.Identifier{Value: tbl.Name}
							break // break out of loop
						}
					}

				}
			}

			if _, ok := optimize.Tables[col.TableName.Value]; !ok {
				optimize.Tables[col.TableName.Value] = []map[string]interface{}{}
			}

			optimize.Tables[col.TableName.Value] = append(optimize.Tables[col.TableName.Value], map[string]interface{}{"column": col.ColumnName.Value, "value": nil})

		}
	case *parser.NotExpr:
		err := ex.opt(cond.(*parser.NotExpr).Expr, optimize, tbls)
		if err != nil {
			return err
		}
	}
	return nil
}

// Row represents a row object
// ID is the page/row id and Row is the actual row
type Row struct {
	ID  int64
	Row *map[string]interface{}
}

// convertRowsToMap converts a slice of rows to a slice of maps
func convertRowsToMap(rows []*Row) []map[string]interface{} {
	var results []map[string]interface{}

	for _, row := range rows {
		results = append(results, *row.Row)
	}

	return results
}

// convertPlanToRows converts a plan to rows
func convertPlanToRows(plan *Plan) []map[string]interface{} {
	var results []map[string]interface{}

	for _, step := range plan.Steps {

		op := ""

		switch step.Operation {
		case FULL_SCAN:
			op = "FULL SCAN"
		case INDEX_SCAN:
			op = "INDEX SCAN"
		}

		results = append(results, map[string]interface{}{"operation": op, "table": step.Table, "column": step.Column, "io": step.IO})
	}

	return results
}

// filter filters rows based on the where clause
func (ex *Executor) filter(where *parser.WhereClause, tbls []*catalog.Table, filteredRows *[]map[string]interface{}, rowIds *[]int64) error {

	if len(tbls) == 0 {
		return errors.New("no tables")
	}

	// gather the tables and columns to check
	optimize := &Optimize{
		Tables: make(map[string][]map[string]interface{}),
	}

	if where != nil {

		// Check for optimizations, such as indexes
		err := ex.opt(where.SearchCondition, optimize, tbls)
		if err != nil {
			return err
		}

		if ex.explaining {
			for tblName, colsValues := range optimize.Tables {

				for _, colValue := range colsValues {
					// Get index
					var tbl *catalog.Table

					for _, t := range tbls {
						if t.Name == tblName {
							tbl = t
							break
						}
					}

					idx := tbl.CheckIndexedColumn(colValue["column"].(string), true)
					if idx == nil {
						// check if non unique index
						idx = tbl.CheckIndexedColumn(colValue["column"].(string), false)
						if idx == nil {
							idx = nil
						}
					}

					io := 0

					if idx != nil {
						// check if value is literal
						key, err := idx.GetBtree().Get([]byte(fmt.Sprintf("%v", colValue["value"])))
						if err != nil {
							return err
						}

						if key != nil {
							for range key.V {
								io++
							}
						}

						ex.plan.Steps = append(ex.plan.Steps, &Step{Operation: INDEX_SCAN, Table: tblName, Column: colValue["column"].(string), IO: int64(io) + idx.GetBtree().Pager.Count()})

					} else {
						// remove from optimize
						delete(optimize.Tables, tblName)
					}

				}
			}

			ex.ResultSetBuffer = shared.CreateTableByteArray(convertPlanToRows(ex.plan), shared.GetHeaders(convertPlanToRows(ex.plan)))

			return nil

		}
	}

	indexedColumns := make(map[string]*catalog.Index)

	var tblIters []*catalog.Iterator

	// For each table
	for _, tbl := range tbls {
		// skip table where index is found
		if _, ok := indexedColumns[tbl.Name]; ok {
			continue
		}

		// Setup new row iterator
		iter := tbl.NewIterator()

		tblIters = append(tblIters, iter)

	}

	invalidIters := 0

	var currentRows []*Row

	if len(optimize.Tables) > 0 {
		for tblName, colsValues := range optimize.Tables {
			var tbl *catalog.Table

			for _, t := range tbls {
				if t.Name == tblName {
					tbl = t
					break
				}
			}

			for _, colValue := range colsValues {
				col := colValue["column"].(string)
				val := colValue["value"]

				var idx *catalog.Index

				idx = tbl.CheckIndexedColumn(col, true)
				if idx == nil {
					// try not unique index
					idx = tbl.CheckIndexedColumn(col, false)
					if idx != nil {
						idx = nil

					}

				}

				if idx != nil {
					idx.GetLock().Lock()
					key, err := idx.GetBtree().Get([]byte(fmt.Sprintf("%v", val)))
					if err != nil {
						idx.GetLock().Unlock()
						return err
					}

					idx.GetLock().Unlock()

					if key != nil {
						for _, v := range key.V {
							int64Str := string(v)
							rRowId, err := strconv.ParseInt(int64Str, 10, 64)
							if err != nil {
								return err
							}

							row, err := tbl.GetRow(rRowId)
							if err != nil {
								return err
							}

							// convert to tablename.columnname
							for k, vv := range row {
								delete(row, k)
								row[fmt.Sprintf("%v.%v", tbl.Name, k)] = vv
							}

							currentRows = append(currentRows, &Row{ID: rRowId, Row: &row})

						}
					}
				}

			}

		}
	}

	for invalidIters < len(tblIters) {
		if invalidIters >= len(tblIters) {
			break
		}

		for i := 0; i < len(tblIters); i++ {
			iter := tblIters[i]

			if iter.Valid() {
				row, err := iter.Next()
				if err != nil {

					invalidIters++
					continue
				}

				// convert row to tablename.columnname

				if i > len(tbls)-1 {
					break
				}

				for k, v := range row {
					delete(row, k)
					row[fmt.Sprintf("%v.%v", tbls[i].Name, k)] = v
				}

				currentRows = append(currentRows, &Row{ID: iter.Current(), Row: &row})

			} else {
				invalidIters++
			}
		}

		currentRowsMap := convertRowsToMap(currentRows)

		if ex.evaluateWhereClause(where, &currentRowsMap, tbls, filteredRows) {

			// add the columns to the new row
			for i, row := range currentRowsMap {

				for k, v := range row {
					// Check if value is time.Time
					if _, ok := v.(time.Time); ok {
						// if so we need to read the schema to get the type
						if len(tbls) > 1 {
							// split the key and get the table name
							tblName := strings.Split(k, ".")[0]

							// get the table
							for _, tbl := range tbls {
								if tbl.Name == tblName {
									// get the column type
									for name, col := range tbl.TableSchema.ColumnDefinitions {
										if name == strings.Split(k, ".")[1] {
											switch col.DataType {
											case "DATE":
												currentRowsMap[i][k] = v.(time.Time).Format("2006-01-02")
											case "TIME":
												currentRowsMap[i][k] = v.(time.Time).Format("15:04:05")
											case "TIMESTAMP", "DATETIME":
												currentRowsMap[i][k] = v.(time.Time).Format("2006-01-02 15:04:05")
											}
										}
									}
								}
							}
						} else {

							tbl := tbls[0]
							// get the column type
							for name, col := range tbl.TableSchema.ColumnDefinitions {

								if name == k {
									switch col.DataType {
									case "DATE":
										currentRowsMap[i][k] = v.(time.Time).Format("2006-01-02")
									case "TIME":
										currentRowsMap[i][k] = v.(time.Time).Format("15:04:05")
									case "TIMESTAMP":
										currentRowsMap[i][k] = v.(time.Time).Format("2006-01-02 15:04:05")
									case "DATETIME":
										currentRowsMap[i][k] = v.(time.Time).Format("2006-01-02 15:04:05")
									}
								}
							}

						}
					}

					if len(tbls) == 1 {
						if strings.Contains(k, ".") {
							// remove tablename
							newKey := strings.Split(k, ".")[1]
							currentRowsMap[i][newKey] = currentRowsMap[i][k]
							delete(currentRowsMap[i], k)
						}
					}

				}
			}

			// create a new row
			newRow := map[string]interface{}{}

			if rowIds != nil {
				if *rowIds == nil {
					*rowIds = []int64{}
				}
			}

			for i := range currentRowsMap {

				if rowIds != nil {
					for j, _ := range currentRows {

						for k, _ := range *currentRows[j].Row {
							if strings.Contains(k, ".") {
								// remove tablename
								newKey := strings.Split(k, ".")[1]
								(*currentRows[j].Row)[newKey] = (*currentRows[j].Row)[k]
								delete(*currentRows[j].Row, k)
							}
						}

						if where != nil {

							if shared.IdenticalMap(*currentRows[j].Row, currentRowsMap[i]) {
								*rowIds = append(*rowIds, currentRows[j].ID)

							}
						}

					}

				}

				for k, v := range currentRowsMap[i] {
					newRow[k] = v
				}
			}

			// check if newRow == map[]
			if len(newRow) > 0 {
				*filteredRows = append(*filteredRows, newRow)

			}

		}
		currentRowsMap = []map[string]interface{}{}

		if where != nil {
			currentRows = []*Row{}
		}

	}

	// if there was no where clause append all row ids
	if where == nil {
		for _, row := range currentRows {
			*rowIds = append(*rowIds, row.ID)
		}

	}

	return nil
}

// evaluateWhereClause evaluates the where clause
func (ex *Executor) evaluateWhereClause(where *parser.WhereClause, rows *[]map[string]interface{}, tbls []*catalog.Table, filteredRows *[]map[string]interface{}) bool {
	// If there is no where clause, we return true
	if where == nil {
		return true
	}

	// If there is a where clause, we evaluate the condition
	return ex.evaluateCondition(where.SearchCondition, rows, tbls, filteredRows)
}

// EvaluateCondition evaluates a condition
func (ex *Executor) evaluateCondition(condition interface{}, rows *[]map[string]interface{}, tbls []*catalog.Table, filteredRows *[]map[string]interface{}) bool {
	// If there is no condition, we return true
	if condition == nil {
		return true
	}

	_, not := condition.(*parser.NotExpr)
	if not {
		condition = condition.(*parser.NotExpr).Expr

	}

	switch condition := condition.(type) {
	case *parser.LogicalCondition:
		switch condition.Op {
		case parser.OP_AND:
			return ex.evaluateCondition(condition.Left, rows, tbls, filteredRows) && ex.evaluateCondition(condition.Right, rows, tbls, filteredRows)
		case parser.OP_OR:
			return ex.evaluateCondition(condition.Left, rows, tbls, filteredRows) || ex.evaluateCondition(condition.Right, rows, tbls, filteredRows)
		case parser.OP_NOT:
			return !ex.evaluateCondition(condition.Right, rows, tbls, filteredRows)
		}

	case *parser.InPredicate:
		// check if left is column spec
		if _, ok := condition.Left.Value.(*parser.ColumnSpecification); ok {
			left := ex.evaluateValueExpression(condition.Left, rows)

			// Check if first value is selectStmt
			if _, ok := condition.Values[0].Value.(*parser.SelectStmt); ok {

				innerRows, err := ex.executeSelectStmt(condition.Values[0].Value.(*parser.SelectStmt), true)
				if err != nil {
					return false
				}

				for _, val := range innerRows {
					if not {

						if left != val[condition.Left.Value.(*parser.ColumnSpecification).ColumnName.Value] {
							return true
						}

					} else {

						if left == val[condition.Left.Value.(*parser.ColumnSpecification).ColumnName.Value] {

							return true
						}

					}
				}

				return false

			}

			for _, val := range condition.Values {

				switch val.Value.(*parser.Literal).Value.(type) {
				case uint64:
					val.Value.(*parser.Literal).Value = int(val.Value.(*parser.Literal).Value.(uint64))
				}

				if not {
					if val.Value.(*parser.Literal).Value != left {

						return true
					}

				} else {
					if val.Value.(*parser.Literal).Value == left {

						return true
					}
				}

			}
		}

	case *parser.IsPredicate:

		if not {
			return false
		}

		if _, ok := condition.Left.Value.(*parser.ColumnSpecification); ok {
			left := ex.evaluateValueExpression(condition.Left, rows)

			if condition.Null {
				return left == nil
			} else {
				return left != nil
			}

		}
	case *parser.BetweenPredicate:
		// check if left is column spec
		if _, ok := condition.Left.Value.(*parser.ColumnSpecification); ok {
			left := ex.evaluateValueExpression(condition.Left, rows)
			right := ex.evaluateValueExpression(condition.Lower, rows)
			upper := ex.evaluateValueExpression(condition.Upper, rows)

			if left == nil {
				return false
			}

			if !not {
				// check if left is a string
				if _, ok := left.(string); ok {
					// check if right is a string
					if _, ok := right.(string); ok {
						return left.(string) >= right.(string) && left.(string) <= upper.(string) // left >= lower && left <= upper
					}

					return false
				}

				// check if left is a float
				if _, ok := left.(float64); ok {
					return left.(float64) >= right.(float64) && left.(float64) <= upper.(float64) // left >= lower && left <= upper
				}

				return left.(int) >= int(right.(uint64)) && left.(int) <= int(upper.(uint64)) // left >= lower && left <= upper
			} else {
				return left.(int) < int(right.(uint64)) || left.(int) > int(upper.(uint64)) // left < lower || left > upper
			}

		}
	case *parser.LikePredicate:
		// check if left is column spec
		if _, ok := condition.Left.Value.(*parser.ColumnSpecification); ok {
			left := ex.evaluateValueExpression(condition.Left, rows)

			pattern := condition.Pattern.Value
			/*
				'%a'
				Matches any string that ends with 'a'. The '%' wildcard matches any sequence of characters, including an empty sequence.

				'%a%'
				Matches any string that contains 'a' anywhere within it. The '%' wildcard before and after 'a' means that 'a' can be preceded or followed by any sequence of characters.

				'a%'
				Matches any string that starts with 'a'. The '%' wildcard after 'a' allows for any sequence of characters after 'a'.

				'a%b'
				Matches any string that starts with 'a' and ends with 'b'. The '%' wildcard in the middle allows for any sequence of characters between 'a' and 'b'.

			*/

			if left == nil {
				return false
			}

			if !not {
				switch {

				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%") && strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// '%a%'
					if strings.Contains(left.(string), strings.TrimPrefix(strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'"), "'%")) {
						return true
					}
				case strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// 'a%'
					if strings.HasPrefix(left.(string), strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'")) {
						return true
					}
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%"):
					// '%a'
					if strings.HasSuffix(left.(string), strings.TrimPrefix(pattern.(*parser.Literal).Value.(string), "'%")) {
						return true
					}
				case len(strings.Split(pattern.(*parser.Literal).Value.(string), "%")) == 2:
					// 'a%b'
					lStr := strings.TrimLeft(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[0], "'")
					rStr := strings.TrimRight(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[1], "'")

					if strings.HasPrefix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), lStr) && strings.HasSuffix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), rStr) {
						return true
					}

				default:
					return false

				}
			} else {
				switch {

				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%") && strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// '%a%'
					if !strings.Contains(left.(string), strings.TrimPrefix(strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'"), "'%")) {
						return true
					}
				case strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// 'a%'
					if !strings.HasPrefix(left.(string), strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'")) {
						return true
					}
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%"):
					// '%a'
					if !strings.HasSuffix(left.(string), strings.TrimPrefix(pattern.(*parser.Literal).Value.(string), "'%")) {
						return true
					}
				case len(strings.Split(pattern.(*parser.Literal).Value.(string), "%")) == 2:
					// 'a%b'
					lStr := strings.TrimLeft(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[0], "'")
					rStr := strings.TrimRight(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[1], "'")

					if !strings.HasPrefix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), lStr) && !strings.HasSuffix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), rStr) {
						return true
					}

				default:
					return false

				}
			}
		}
	case *parser.ExistsPredicate:
		// check subquery

		// Pass outer table to exists subquery
		/*
			SELECT *
			FROM users
			WHERE EXISTS (
			    SELECT 1
			    FROM posts
			    WHERE users.user_id + posts.user_id = 5
			);
		*/
		for _, tbl := range tbls {
			//condition.Expr.Value.(*parser.SelectStmt).TableExpression.FromClause.Tables = append(condition.Expr.Value.(*parser.SelectStmt).TableExpression.FromClause.Tables, &parser.Table{
			//	Name: &parser.Identifier{Value: tbl.Name},
			//})

			// push to start of condition.Expr.Value.(*parser.SelectStmt).TableExpression.FromClause.Tables
			condition.Expr.Value.(*parser.SelectStmt).TableExpression.FromClause.Tables = append([]*parser.Table{&parser.Table{
				Name: &parser.Identifier{Value: tbl.Name},
			}}, condition.Expr.Value.(*parser.SelectStmt).TableExpression.FromClause.Tables...)
		}

		r, err := ex.executeSelectStmt(condition.Expr.Value.(*parser.SelectStmt), true)
		if err != nil {
			return false
		}

		if not {
			// if not exists
			// if there are no results return true
			if len(r) == 0 {
				return true
			} else {
				return false
			}
		}

		// when there is no not, we add to filteredRows
		if len(r) > 0 {

			// check if any results compare to *filteredRows
			// if so skip
			if len(*filteredRows) > 0 {
				for _, row := range *filteredRows {
					for _, rr := range r {
						for k, v := range rr {
							if row[k] == v {
								return false
							} else {
								if row[k] != v {
									return false
								}
							}
						}

						*filteredRows = append(*filteredRows, rr)
						return true
					}

					return true
				}
			} else {
				*filteredRows = append(*filteredRows, r...)
				return false
			}

			return false
		} else {
			return false
		}

	case *parser.ComparisonPredicate:

		left := ex.evaluateValueExpression(condition.Left, rows)
		right := ex.evaluateValueExpression(condition.Right, rows)

		// check if right is value expression
		if _, ok := condition.Right.Value.(*parser.ValueExpression); ok {

			// check if right is subquery
			if _, ok := condition.Right.Value.(*parser.ValueExpression).Value.(*parser.SelectStmt); ok {
				rows, err := ex.executeSelectStmt(condition.Right.Value.(*parser.ValueExpression).Value.(*parser.SelectStmt), true)
				if err != nil {
					return false
				}

				// get first key
				for k, _ := range rows[0] {
					right = rows[0][k]
					break
				}

			}
		}

		switch left.(type) {
		case int:
			// Check if right is not int
			if _, ok := right.(int); !ok {
				// check if right is nil
				if right == nil {
					return false
				}

				// check if right is string
				if _, ok := right.(string); ok {
					return false
				}

				right = int(right.(uint64))

			}
		case float64:
			// Check if right is not float64
			if _, ok := right.(string); ok {
				return false
			}

			right = float64(right.(uint64))

		}

		switch condition.Op {
		case parser.OP_EQ:

			if !not {
				return left == right
			} else {
				return left != right
			}

		case parser.OP_NEQ:
			if !not {
				return left != right
			} else {
				return left == right
			}

		case parser.OP_LT:
			if !not {
				switch left.(type) {
				case int:
					return left.(int) < right.(int)
				case float64:
					return left.(float64) < right.(float64)
				case uint64:
					return left.(uint64) < right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) < right.(string)
					}

					return false
				}
			} else {
				switch left.(type) {
				case int:
					return left.(int) >= right.(int)
				case float64:
					return left.(float64) >= right.(float64)
				case uint64:
					return left.(uint64) >= right.(uint64)
				case string:

					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) >= right.(string)
					}

					return false
				}
			}
		case parser.OP_LTE:
			if !not {
				switch left.(type) {
				case int:
					return left.(int) <= right.(int)
				case float64:
					return left.(float64) <= right.(float64)
				case uint64:
					return left.(uint64) <= right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) <= right.(string)
					}

					return false
				}

			} else {
				switch left.(type) {
				case int:
					return left.(int) > right.(int)
				case float64:
					return left.(float64) > right.(float64)
				case uint64:
					return left.(uint64) > right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) > right.(string)
					}

					return false
				}
			}
		case parser.OP_GT:
			if !not {
				switch left.(type) {
				case int:
					return left.(int) > right.(int)
				case float64:
					return left.(float64) > right.(float64)
				case uint64:
					return left.(uint64) > right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) > right.(string)
					}

					return false
				}
			} else {
				switch left.(type) {
				case int:
					return left.(int) <= right.(int)
				case float64:
					return left.(float64) <= right.(float64)
				case uint64:
					return left.(uint64) <= right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) <= right.(string)
					}

					return false
				}

			}
		case parser.OP_GTE:
			if !not {
				switch left.(type) {
				case int:
					return left.(int) >= right.(int)
				case float64:
					return left.(float64) >= right.(float64)
				case uint64:
					return left.(uint64) >= right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) >= right.(string)
					}

					return false
				}
			} else {
				switch left.(type) {
				case int:
					return left.(int) < right.(int)
				case float64:
					return left.(float64) < right.(float64)
				case uint64:
					return left.(uint64) < right.(uint64)
				case string:
					// check if right is string
					if _, ok := right.(string); ok {
						return left.(string) < right.(string)
					}

					return false
				}
			}
		}
	default:

		return false

	}

	return false
}

// evaluateCaseExpr evaluates a case expression with a where clause
func (ex *Executor) evaluateCaseExpr(expr *parser.CaseExpr, rows *[]map[string]interface{}) (string, error) {
	// If there is no where clause, we return true
	if expr == nil {
		return "", nil
	}

	// If there is a where clause, we evaluate the condition
	return ex.evaluateCaseCondition(expr, rows)

}

// EvaluateCaseCondition evaluates a case condition
func (ex *Executor) evaluateCaseCondition(expr *parser.CaseExpr, rows *[]map[string]interface{}) (string, error) {
	// If there is no condition, we return true
	if expr == nil {
		return "", nil
	}

	col := "" // column name of where we will be updating

	// Check first when clause for left column
	switch expr.WhenClauses[0].Condition.(type) {
	case *parser.ComparisonPredicate:
		col = expr.WhenClauses[0].Condition.(*parser.ComparisonPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
	case *parser.InPredicate:
		col = expr.WhenClauses[0].Condition.(*parser.InPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
	case *parser.BetweenPredicate:
		col = expr.WhenClauses[0].Condition.(*parser.BetweenPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
	case *parser.LikePredicate:
		col = expr.WhenClauses[0].Condition.(*parser.LikePredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
	case *parser.IsPredicate:
		col = expr.WhenClauses[0].Condition.(*parser.IsPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value

	}

	for _, when := range expr.WhenClauses {
		if when.Condition != nil {
			if ex.evaluateCondition(when.Condition, rows, nil, nil) {
				for i, row := range *rows {
					for k, _ := range row {
						if k == col {
							(*rows)[i][col] = when.Result.(*parser.ValueExpression).Value.(*parser.Literal).Value
						}
					}
				}

			}
		}
	}

	return col, nil
}

// EvaluateValueExpression evaluates a value expression
func (ex *Executor) evaluateValueExpression(expr *parser.ValueExpression, rows *[]map[string]interface{}) interface{} {
	switch expr := expr.Value.(type) {
	case *parser.CaseExpr:
		// check if left is column spec
		col, err := ex.evaluateCaseExpr(expr, rows)
		if err != nil {
			return false
		}

		for _, row := range *rows {
			for k, v := range row {
				if k == col {
					return v
				}
			}

		}

	case *shared.GenUUID:
		return shared.GenerateUUID()
	case *shared.SysDate, *shared.SysTimestamp, *shared.SysTime:
		return time.Now()

	case *parser.UpperFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = strings.ToUpper(v.(string))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.LowerFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = strings.ToLower(v.(string))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.RoundFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(float64); ok {

						newRow[k] = math.Round(v.(float64))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					} else if _, ok := v.(int); ok {
						newRow[k] = math.Round(float64(v.(int)))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					} else if _, ok := v.(uint64); ok {
						newRow[k] = math.Round(float64(v.(uint64)))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.ReverseFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = shared.ReverseString(v.(string))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}
		}
	case *parser.LengthFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = len(v.(string)) - 2
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}

	case *parser.PositionFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.In.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = (strings.Index(v.(string), strings.TrimSuffix(strings.TrimPrefix(expr.Arg.(*parser.ValueExpression).Value.(*parser.Literal).Value.(string), "'"), "'"))) + 1

						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.SubstrFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						start := int(expr.StartPos.Value.(uint64))

						if start == 1 {
							start = 0
						}

						end := int(expr.Length.Value.(uint64))

						newRow[k] = strings.TrimPrefix(strings.TrimSuffix(v.(string), "'"), "'")[start:end]

						newRow[k] = fmt.Sprintf("'%s'", newRow[k])

						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.ConcatFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Args[0].(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						for _, arg := range expr.Args[1:] {
							if _, ok := arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification); ok {
								var newStr []string
								newStr = append(newStr, strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'"))
								newStr = append(newStr, strings.TrimSuffix(strings.TrimPrefix(newRow[arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value].(string), "'"), "'"))
								newRow[k] = fmt.Sprintf("'%s'", strings.Join(newStr, ""))
							} else {
								var newStr []string
								newStr = append(newStr, strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'"))
								newStr = append(newStr, strings.TrimSuffix(strings.TrimPrefix(arg.(*parser.ValueExpression).Value.(*parser.Literal).Value.(string), "'"), "'"))
								newRow[k] = fmt.Sprintf("'%s'", strings.Join(newStr, ""))
							}
						}

						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}
	case *parser.TrimFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Arg.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// check if row value is string
					if _, ok := v.(string); ok {
						newRow[k] = fmt.Sprintf("'%s'", strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(v.(string), "'"), "'")))
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}

				}
			}

		}

	case *parser.CoalesceFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Args[0].(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					// Value should be nil
					if v == nil {
						newRow[k] = expr.Value.(*parser.ValueExpression).Value.(*parser.Literal).Value
						*rows = append(*rows, newRow)
						*rows = append((*rows)[:i], (*rows)[i+1:]...)
						return newRow[k]
					}
				}
			}

		}
	case *parser.CastFunc:
		for i, row := range *rows {

			newRow := map[string]interface{}{}
			for k, v := range row {
				// trim off the tablename if it exists

				if strings.Contains(k, ".") {
					newRow[strings.Split(k, ".")[1]] = v
				} else {
					newRow[k] = v

				}
			}

			for k, v := range newRow {

				if k == expr.Expr.(*parser.ValueExpression).Value.(*parser.ColumnSpecification).ColumnName.Value {
					switch expr.DataType.Value {
					case "INT", "INTEGER", "SMALLINT":
						// check if row value is string
						if _, ok := v.(string); ok {
							// Convert string to int
							intVal, err := strconv.Atoi(v.(string))
							if err != nil {
								return nil
							}

							newRow[k] = intVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]

						}
					case "FLOAT", "DOUBLE", "DECIMAL", "REAL", "NUMERIC":
						// check if row value is string
						if _, ok := v.(string); ok {
							// Convert string to float
							floatVal, err := strconv.ParseFloat(v.(string), 64)
							if err != nil {
								return nil
							}

							newRow[k] = floatVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]
						}
					case "DATE":
						// check if row value is string
						if _, ok := v.(string); ok {
							// Convert string to time.Time
							dateVal, err := time.Parse("2006-01-02", v.(string))
							if err != nil {
								return nil
							}

							newRow[k] = dateVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]
						}
					case "DATETIME", "TIMESTAMP":
						// check if row value is string
						if _, ok := v.(string); ok {
							// Convert string to time.Time
							dateVal, err := time.Parse("2006-01-02 15:04:05", v.(string))
							if err != nil {
								return nil
							}

							newRow[k] = dateVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]

						}
					case "TIME":
						// check if row value is string
						if _, ok := v.(string); ok {
							// Convert string to time.Time
							dateVal, err := time.Parse("15:04:05", v.(string))
							if err != nil {
								return nil
							}

							newRow[k] = dateVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]
						}
					case "BOOL", "BOOLEAN":
						// check if row value is string
						if _, ok := v.(string); ok {
							if v.(string) == "T" || v.(string) == "t" {
								v = "true"
							} else if v.(string) == "F" || v.(string) == "f" {
								v = "false"
							} else if v.(string) == "'true'" {
								v = "true"
							} else if v.(string) == "'false'" {
								v = "false"
							}

							// Convert string to bool
							boolVal, err := strconv.ParseBool(v.(string))
							if err != nil {
								return nil
							}

							newRow[k] = boolVal

							*rows = append(*rows, newRow)

							*rows = append((*rows)[:i], (*rows)[i+1:]...)

							return newRow[k]

						}
					}

				}
			}

		}

	case *parser.Literal:
		return expr.Value
	case *parser.ColumnSpecification:

		if expr.TableName == nil {
			for i, row := range *rows {
				newRow := map[string]interface{}{}
				for k, v := range row {
					// trim off the tablename if it exists

					if strings.Contains(k, ".") {
						newRow[strings.Split(k, ".")[1]] = v
					} else {
						newRow[k] = v

					}
				}
				*rows = append(*rows, newRow)
				*rows = append((*rows)[:i], (*rows)[i+1:]...)
			}
		}
		for _, row := range *rows {
			// check if tablename.columnname exists
			if expr.TableName != nil {
				if _, ok := row[fmt.Sprintf("%v.%v", expr.TableName.Value, expr.ColumnName.Value)]; ok {
					return row[fmt.Sprintf("%v.%v", expr.TableName.Value, expr.ColumnName.Value)]
				}
			}
			if _, ok := row[expr.ColumnName.Value]; ok {

				return row[expr.ColumnName.Value]
			}

		}

		return nil

	case *parser.BinaryExpression:
		var val interface{}
		err := evaluateBinaryExpression(expr, &val, rows)
		if err != nil {
			return nil
		}

		return val
	case *parser.Variable:
		// check if variable exists
		if _, ok := ex.vars[expr.VariableName.Value]; ok {
			return ex.vars[expr.VariableName.Value].Value
		}
	}

	return nil
}

// getFirstLeftBinaryExpressionColumn gets the first left binary expression column
func getFirstLeftBinaryExpressionColumn(expr *parser.BinaryExpression) *parser.ColumnSpecification {
	if _, ok := expr.Left.(*parser.ColumnSpecification); ok {
		return expr.Left.(*parser.ColumnSpecification)
	}

	if _, ok := expr.Left.(*parser.BinaryExpression); ok {
		return getFirstLeftBinaryExpressionColumn(expr.Left.(*parser.BinaryExpression))
	}

	return nil

}

// evaluateBinaryExpression evaluates a binary expression
func evaluateBinaryExpression(expr *parser.BinaryExpression, val *interface{}, rows *[]map[string]interface{}) error {

	left := expr.Left
	right := expr.Right
	var row map[string]interface{}

	// Check if left is column spec
	if _, ok := left.(*parser.ColumnSpecification); ok {

		if left.(*parser.ColumnSpecification).TableName == nil {
			for i, r := range *rows {
				newRow := map[string]interface{}{}
				for k, v := range r {
					// trim off the tablename if it exists

					if strings.Contains(k, ".") {
						newRow[strings.Split(k, ".")[1]] = v
					} else {
						newRow[k] = v

					}
				}
				*rows = append(*rows, newRow)
				*rows = append((*rows)[:i], (*rows)[i+1:]...)
			}

		}

		for _, r := range *rows {
			if _, ok := r[left.(*parser.ColumnSpecification).ColumnName.Value]; ok {
				row = r
				break
			}
		}

		left = &parser.Literal{Value: row[left.(*parser.ColumnSpecification).ColumnName.Value]}
	} else if _, ok := left.(*parser.AggregateFunc); ok {
		// Check if left is aggregate function

		var columns []string
		err := evaluateAggregate(left.(*parser.AggregateFunc), rows, &columns, nil)
		if err != nil {
			return err
		}

		// Get first row value
		for _, r := range *rows {
			left = &parser.Literal{Value: r[columns[0]]}
		}

	}

	// Check if left is binary expr
	if _, ok := left.(*parser.BinaryExpression); ok {
		var valInner interface{}
		err := evaluateBinaryExpression(left.(*parser.BinaryExpression), &valInner, rows)
		if err != nil {
			return err
		}

		left = &parser.Literal{Value: valInner}

	}

	if _, ok := right.(*parser.BinaryExpression); ok {
		var rVal interface{}
		err := evaluateBinaryExpression(right.(*parser.BinaryExpression), &rVal, rows)
		if err != nil {
			return err
		}

		right = &parser.Literal{Value: rVal}
	}

	switch left := left.(type) {

	case *parser.Literal:
		switch right := right.(type) {
		case *parser.Literal:
			switch expr.Op {
			case parser.OP_PLUS:

				switch left.Value.(type) {
				case int:
					switch right.Value.(type) {
					case uint64:
						*val = left.Value.(int) + int(right.Value.(uint64))
					case int:
						*val = left.Value.(int) + right.Value.(int)
					case int64:
						*val = left.Value.(int) + int(right.Value.(int64))
					case float64:
						*val = left.Value.(int) + int(right.Value.(float64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case uint64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(uint64)) + right.Value.(int)
					case int64:
						*val = int(left.Value.(uint64)) + int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(uint64)) + int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(uint64)) + int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

					}
				case int64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(int64)) + right.Value.(int)
					case int64:
						*val = int(left.Value.(int64)) + int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(int64)) + int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(int64)) + int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case float64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(float64)) + right.Value.(int)
					case int64:
						*val = int(left.Value.(float64)) + int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(float64)) + int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(float64)) + int(right.Value.(uint64))
					default:
						return errors.New("unsupported type ")
					}
				default:
					return errors.New("unsupported type ")

				}

			case parser.OP_MINUS:
				switch left.Value.(type) {
				case int:
					switch right.Value.(type) {
					case uint64:
						*val = left.Value.(int) - int(right.Value.(uint64))
					case int:
						*val = left.Value.(int) - right.Value.(int)
					case int64:
						*val = left.Value.(int) - int(right.Value.(int64))
					case float64:
						*val = left.Value.(int) - int(right.Value.(float64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case uint64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(uint64)) - right.Value.(int)
					case int64:
						*val = int(left.Value.(uint64)) - int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(uint64)) - int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(uint64)) - int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

					}
				case int64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(int64)) - right.Value.(int)
					case int64:
						*val = int(left.Value.(int64)) - int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(int64)) - int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(int64)) - int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case float64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(float64)) - right.Value.(int)
					case int64:
						*val = int(left.Value.(float64)) - int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(float64)) - int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(float64)) - int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				default:
					return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

				}
			case parser.OP_MULT:
				switch left.Value.(type) {
				case int:
					switch right.Value.(type) {
					case uint64:
						*val = left.Value.(int) * int(right.Value.(uint64))
					case int:
						*val = left.Value.(int) * right.Value.(int)
					case int64:
						*val = left.Value.(int) * int(right.Value.(int64))
					case float64:
						*val = left.Value.(int) * int(right.Value.(float64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case uint64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(uint64)) * right.Value.(int)
					case int64:
						*val = int(left.Value.(uint64)) * int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(uint64)) * int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(uint64)) * int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

					}
				case int64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(int64)) * right.Value.(int)
					case int64:
						*val = int(left.Value.(int64)) * int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(int64)) * int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(int64)) * int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case float64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(float64)) * right.Value.(int)
					case int64:
						*val = int(left.Value.(float64)) * int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(float64)) * int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(float64)) * int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				default:
					return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

				}
			case parser.OP_DIV:
				switch left.Value.(type) {
				case int:
					switch right.Value.(type) {
					case uint64:
						*val = left.Value.(int) / int(right.Value.(uint64))
					case int:
						*val = left.Value.(int) / right.Value.(int)
					case int64:
						*val = left.Value.(int) / int(right.Value.(int64))
					case float64:
						*val = left.Value.(int) / int(right.Value.(float64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case uint64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(uint64)) / right.Value.(int)
					case int64:
						*val = int(left.Value.(uint64)) / int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(uint64)) / int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(uint64)) / int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

					}
				case int64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(int64)) / right.Value.(int)
					case int64:
						*val = int(left.Value.(int64)) / int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(int64)) / int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(int64)) / int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				case float64:
					switch right.Value.(type) {
					case int:
						*val = int(left.Value.(float64)) / right.Value.(int)
					case int64:
						*val = int(left.Value.(float64)) / int(right.Value.(int64))
					case float64:
						*val = int(left.Value.(float64)) / int(right.Value.(float64))
					case uint64:
						*val = int(left.Value.(float64)) / int(right.Value.(uint64))
					default:
						return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())
					}
				default:
					return errors.New("unsupported type " + reflect.TypeOf(left.Value).String())

				}
			default:
				return errors.New("unsupported operator ")

			}

		}
	}

	return nil
}

// orderBy orders the results
func (ex *Executor) orderBy(results []map[string]interface{}, orderBy *parser.OrderByClause) ([]map[string]interface{}, error) {
	if orderBy == nil {
		return results, nil
	}

	if len(orderBy.OrderByExpressions) == 0 {
		return results, nil
	}

	// Get the column name
	colName := orderBy.OrderByExpressions[0].Value.(*parser.ColumnSpecification).ColumnName.Value

	// Get the order
	order := orderBy.Order

	// Define a custom sort function
	less := func(i, j int) bool {
		// You may want to add error checking here
		switch results[i][colName].(type) {
		case int:
			return results[i][colName].(int) < results[j][colName].(int)
		case int64:
			return results[i][colName].(int64) < results[j][colName].(int64)
		case float64:
			return results[i][colName].(float64) < results[j][colName].(float64)
		case string:
			return strings.Compare(results[i][colName].(string), results[j][colName].(string)) < 0
		}
		return false
	}

	// Sort the results
	if order == parser.ASC {
		sort.SliceStable(results, less)
	} else {
		// For descending order, we can use the same function but negate the result
		switch results[0][colName].(type) {
		case int:
			sort.SliceStable(results, func(i, j int) bool {
				return !less(i, j)
			})
		case int64:
			sort.SliceStable(results, func(i, j int) bool {
				return !less(i, j)
			})
		case float64:
			sort.SliceStable(results, func(i, j int) bool {
				return !less(i, j)
			})
		case string:
			sort.SliceStable(results, func(i, j int) bool {
				return !less(i, j)
			})
		default:
			return nil, errors.New("unsupported data type")
		}
	}

	return results, nil
}

// Clear clears the result set buffer
func (ex *Executor) Clear() {
	ex.ResultSetBuffer = nil
}

// rollback rolls back a transaction
func (ex *Executor) rollback() error {
	if !ex.TransactionBegun {
		return errors.New("no transaction begun")
	}

	ex.TransactionBegun = false

	for _, tx := range ex.Transaction.Statements {
		if tx.Commited {
			// If a transaction is commited we can rollback the transaction
			// This allows for database consistency
			switch stmt := tx.Stmt.(type) { // only Insert, Update, Delete, statements can be rolled back
			case *parser.InsertStmt:
				tbl := ex.ch.Database.GetTable(stmt.TableName.Value)

				if tbl == nil {
					return errors.New("table does not exist")
				}

				// In tx.Before for insert we have the row ids that were inserted, thus making it easy to remove them
				for _, row := range tx.Rollback.Rows {
					err := tbl.Rows.DeletePage(row.RowId)
					if err != nil {
						return err
					}
				}
			case *parser.UpdateStmt:
				tbl := ex.ch.Database.GetTable(stmt.TableName.Value)

				if tbl == nil {
					return errors.New("table does not exist")
				}

				// In tx.Before for update we have the row ids and their previous entire rows thus making it easy to write back the previous value

				for _, row := range tx.Rollback.Rows {
					// en
					encoded, err := catalog.EncodeRow(row.Row)
					if err != nil {
						return err
					}

					err = tbl.Rows.WriteTo(row.RowId, encoded)
					if err != nil {
						return err
					}
				}
			case *parser.DeleteStmt:
				tbl := ex.ch.Database.GetTable(stmt.TableName.Value)

				if tbl == nil {
					return errors.New("table does not exist")
				}

				// In tx.Before for delete we have the row ids and their previous entire rows thus making it easy to write back the previous value
				for _, row := range tx.Rollback.Rows {
					// en
					encoded, err := catalog.EncodeRow(row.Row)
					if err != nil {
						return err
					}

					err = tbl.Rows.WriteTo(row.RowId, encoded)
					if err != nil {
						return err
					}

				}
			}
		}
	}

	ex.Transaction = nil // clear transaction

	return nil
}

// Recover recovers an AriaSQL instance from a WAL file
func (ex *Executor) Recover(asts []interface{}) error {

	// check if data directory exists
	if _, err := os.Stat(ex.aria.Config.DataDir); !os.IsNotExist(err) {

		err := os.RemoveAll(fmt.Sprintf("%s%sdatabases", ex.aria.Config.DataDir, shared.GetOsPathSeparator()))
		if err != nil {
			return err
		}
	}

	if _, err := os.Stat(ex.aria.Config.DataDir); !os.IsNotExist(err) {

		err := os.RemoveAll(fmt.Sprintf("%s%susers.usrs", ex.aria.Config.DataDir, shared.GetOsPathSeparator()))
		if err != nil {
			return err
		}
	}

	aria, err := core.New(&core.Config{
		DataDir: ex.aria.Config.DataDir,
	})
	if err != nil {
		return err
	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		return err
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin") // will bypass privileges as executor is set to recover
	if user == nil {
		return fmt.Errorf("admin user not found")
	}

	ex.aria = aria
	ex.ch = aria.OpenChannel(user)

	for _, stmt := range asts {
		err := ex.Execute(stmt)
		if err != nil {
			return err
		}
	}

	ex.aria.Close()

	return nil
}

// SetRecover sets the recover flag
func (ex *Executor) SetRecover(rec bool) {
	ex.recover = rec
}

// GetResultSet returns the result set buffer
func (ex *Executor) GetResultSet() []byte {
	return ex.ResultSetBuffer
}

// SetJsonOutput sets the json output flag
func (ex *Executor) SetJsonOutput(jsonOutput bool) {
	ex.json = jsonOutput
}
