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
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Executor is the main executor structure
type Executor struct {
	aria             *core.AriaSQL // AriaSQL instance pointer
	ch               *core.Channel // Channel pointer
	Transaction      *Transaction  // Transaction statements
	TransactionBegun bool          // Transaction begun
	resultSetBuffer  []byte        // Result set buffer
}

// Transaction represents a transaction
type Transaction struct {
	Statements []*TransactionStmt
}

// TransactionStmt represents a transaction statement
type TransactionStmt struct {
	Stmt     interface{} // The statement, (insert, update, delete)
	Commited bool        // Whether the statement has been commited
	Rollback *Rollback   // Rollback data
}

// Rollback represents a transaction rollback
type Rollback struct {
	Rows []*Before
}

// Before represents the state of a row before a transaction
type Before struct {
	RowId int64
	Row   map[string]interface{}
}

// New creates a new Executor
func New(aria *core.AriaSQL, ch *core.Channel) *Executor {
	return &Executor{ch: ch, aria: aria}
}

// Execute executes a statement
func (ex *Executor) Execute(stmt parser.Statement) error {

	switch s := stmt.(type) {
	case *parser.RollbackStmt:
		// Check if the database is the system database

		// Check user has the privilege to rollback
		if !ex.ch.User.HasPrivilege("", "", []shared.PrivilegeAction{shared.PRIV_ROLLBACK}) {
			return errors.New("user does not have the privilege to ROLLBACK on system") // Transactions are system wide
		}

		if !ex.TransactionBegun {
			return errors.New("no transaction begun")
		}

		err := ex.rollback()
		if err != nil {
			return err

		}

		return nil
	case *parser.CommitStmt:
		if !ex.ch.User.HasPrivilege("", "", []shared.PrivilegeAction{shared.PRIV_COMMIT}) {
			return errors.New("user does not have the privilege to COMMIT on system") // Transactions are system wide
		}

		if !ex.TransactionBegun {
			return errors.New("no transaction begun")
		}

		for i, tx := range ex.Transaction.Statements {

			err := ex.Execute(tx)
			if err != nil {
				err = ex.rollback()
				if err != nil {
					return err
				} // Rollback the transaction

				return err
			}

			ex.Transaction.Statements[i].Commited = true

		}

		ex.TransactionBegun = false

		return nil
	case *parser.BeginStmt:
		if !ex.ch.User.HasPrivilege("", "", []shared.PrivilegeAction{shared.PRIV_COMMIT}) {
			return errors.New("user does not have the privilege to BEGIN on system") // Transactions are system wide
		}

		if ex.TransactionBegun {
			return errors.New("transaction already begun")
		}

		ex.TransactionBegun = true

		ex.Transaction = &Transaction{Statements: []*TransactionStmt{}} // Initialize the transaction

		return nil
	case *parser.CreateDatabaseStmt:
		if !ex.ch.User.HasPrivilege("", "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to CREATE on system")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		return ex.aria.Catalog.CreateDatabase(s.Name.Value)
	case *parser.CreateTableStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to CREATE on system for database " + ex.ch.Database.Name)
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		err = ex.ch.Database.CreateTable(s.TableName.Value, s.TableSchema)
		if err != nil {
			return err
		}

		return nil

	case *parser.DropTableStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to DROP on system for database " + ex.ch.Database.Name)
		}

		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		err = ex.ch.Database.DropTable(s.TableName.Value)
		if err != nil {
			return err
		}

		return nil
	case *parser.CreateIndexStmt:

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to CREATE on system for database " + ex.ch.Database.Name)
		}

		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		// convert *parser.Identifier to []string
		var columns []string
		for _, col := range s.ColumnNames {
			columns = append(columns, col.Value)
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		err = tbl.CreateIndex(s.IndexName.Value, columns, s.Unique)
		if err != nil {
			return err
		}

		return nil
	case *parser.DropIndexStmt:
		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to DRP{ on system for database " + ex.ch.Database.Name)
		}

		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		err = tbl.DropIndex(s.IndexName.Value)
		if err != nil {
			return err
		}

		return nil
	case *parser.InsertStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		tbl := ex.ch.Database.GetTable(s.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to INSERT on system for database " + ex.ch.Database.Name + " and table " + s.TableName.Value)
		}

		var rows []map[string]interface{}

		for _, row := range s.Values {
			data := map[string]interface{}{}
			for i, col := range s.ColumnNames {
				data[col.Value] = row[i].Value
			}
			rows = append(rows, data)

		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		rowIds, insertedRows, err := tbl.Insert(rows)
		if err != nil {
			return err
		}

		if ex.TransactionBegun {
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})

			for i, rowId := range rowIds {
				ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
					RowId: rowId,
					Row:   insertedRows[i],
				})
			}
		}

		return nil
	case *parser.UseStmt:
		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		db := ex.aria.Catalog.GetDatabase(s.DatabaseName.Value)
		if db == nil {
			return errors.New("database does not exist")
		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		ex.ch.Database = db
		return nil
	case *parser.DropDatabaseStmt:

		if !ex.ch.User.HasPrivilege(stmt.(*parser.DropDatabaseStmt).Name.Value, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to INSERT on system for database " + stmt.(*parser.DropDatabaseStmt).Name.Value)
		}

		err := ex.aria.Catalog.DropDatabase(s.Name.Value)
		if err != nil {
			return err
		}

		err = ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		// if the database is the current database, set the current database to nil
		if ex.ch.Database.Name == s.Name.Value {
			ex.ch.Database = nil
		}

		return nil

	case *parser.SelectStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")

		}

		_, err := ex.executeSelectStmt(s, false)
		if err != nil {
			return err
		}

		return nil
	case *parser.UpdateStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")

		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		rowIds, updatedRows, err := ex.executeUpdateStmt(s)
		if err != nil {
			return err
		}

		if ex.TransactionBegun {
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})

			for i, rowId := range rowIds {
				ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
					RowId: rowId,
					Row:   updatedRows[i],
				})
			}
		}

		return nil

	case *parser.DeleteStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")

		}

		err := ex.aria.WAL.Append(ex.aria.WAL.Encode(&stmt))
		if err != nil {
			return err
		}

		rowIds, deletedRows, err := ex.executeDeleteStmt(s)
		if err != nil {
			return err
		}

		if ex.TransactionBegun {
			ex.Transaction.Statements = append(ex.Transaction.Statements, &TransactionStmt{
				Stmt:     s,
				Commited: false,
				Rollback: &Rollback{Rows: []*Before{}},
			})

			for i, rowId := range rowIds {
				ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows = append(ex.Transaction.Statements[len(ex.Transaction.Statements)-1].Rollback.Rows, &Before{
					RowId: rowId,
					Row:   deletedRows[i],
				})
			}
		}

		return nil
	case *parser.CreateUserStmt:

		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to CREATE on system")
		}

		if ex.TransactionBegun {
			return errors.New("CREATE, ALTER, DROP statements not allowed in a transaction")
		}
		err := ex.aria.Catalog.CreateNewUser(s.Username.Value, s.Password.Value.(string))
		if err != nil {
			return err
		}

	case *parser.DropUserStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_CREATE}) {
			return errors.New("user does not have the privilege to DROP on system")
		}

		if ex.TransactionBegun {
			return errors.New("CREATE, ALTER, DROP statements not allowed in a transaction")
		}

		err := ex.aria.Catalog.DropUser(s.Username.Value)
		if err != nil {
			return err
		}

	case *parser.GrantStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_GRANT}) {
			return errors.New("user does not have the privilege to GRANT on system")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if len(strings.Split(s.PrivilegeDefinition.Object.Value, ".")) < 2 {
			return errors.New("invalid object")
		}

		databaseName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[0]
		tableName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[1]

		priv := &catalog.Privilege{
			DatabaseName:     databaseName,
			TableName:        tableName,
			PrivilegeActions: nil,
		}

		for _, action := range s.PrivilegeDefinition.Actions {
			priv.PrivilegeActions = append(priv.PrivilegeActions, action)
		}

		err := ex.aria.Catalog.GrantPrivilegeToUser(s.PrivilegeDefinition.Grantee.Value, priv)
		if err != nil {
			return err
		}

	case *parser.RevokeStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_REVOKE}) {
			return errors.New("user does not have the privilege to REVOKE on system")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if len(strings.Split(s.PrivilegeDefinition.Object.Value, ".")) < 2 {
			return errors.New("invalid object")
		}

		databaseName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[0]
		tableName := strings.Split(s.PrivilegeDefinition.Object.Value, ".")[1]

		priv := &catalog.Privilege{
			DatabaseName:     databaseName,
			TableName:        tableName,
			PrivilegeActions: nil,
		}

		for _, action := range s.PrivilegeDefinition.Actions {
			priv.PrivilegeActions = append(priv.PrivilegeActions, action)
		}

		err := ex.aria.Catalog.RevokePrivilegeFromUser(s.PrivilegeDefinition.Revokee.Value, priv)
		if err != nil {
			return err
		}

	case *parser.ShowStmt:
		if !ex.ch.User.HasPrivilege(ex.ch.Database.Name, "", []shared.PrivilegeAction{shared.PRIV_SHOW}) {
			return errors.New("user does not have the privilege to SHOW on system")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		switch s.ShowType {
		case parser.SHOW_DATABASES:
			databases := ex.aria.Catalog.GetDatabases()
			results := []map[string]interface{}{
				{"Databases": databases},
			}

			ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))
			return nil
		case parser.SHOW_TABLES:
			if ex.ch.Database == nil {
				return errors.New("no database selected")
			}

			tables := ex.ch.Database.GetTables()
			results := []map[string]interface{}{
				{"Tables": tables},
			}

			ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

			return nil

		case parser.SHOW_USERS:
			users := ex.aria.Catalog.GetUsers()
			results := []map[string]interface{}{
				{"Users": users},
			}

			ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

			return nil
		default:
			return errors.New("unsupported show type")
		}
	case *parser.AlterUserStmt:
		if !ex.ch.User.HasPrivilege("*", "*", []shared.PrivilegeAction{shared.PRIV_ALTER}) {
			return errors.New("user does not have the privilege to ALTER on system")
		}

		if ex.TransactionBegun {
			return errors.New("USE, CREATE, ALTER, DROP, GRANT, REVOKE, SHOW statements not allowed in a transaction")
		}

		if s.SetType == parser.ALTER_USER_SET_PASSWORD {
			err := ex.aria.Catalog.AlterUserPassword(s.Username.Value, s.Value.Value.(string))
			if err != nil {
				return err
			}
		} else if s.SetType == parser.ALTER_USER_SET_USERNAME {
			err := ex.aria.Catalog.AlterUserUsername(s.Username.Value, s.Value.Value.(string))
			if err != nil {
				return err
			}
		} else {
			return errors.New("unsupported set type for alter user")

		}
	default:
		return errors.New("unsupported statement")

	}

	return errors.New("unsupported statement")
}

// rollback rolls back the transaction
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

	return nil
}

// executeDeleteStmt executes a delete statement
func (ex *Executor) executeDeleteStmt(stmt *parser.DeleteStmt) ([]int64, []map[string]interface{}, error) {
	var tbles []*catalog.Table // Table list
	var rowIds []int64         // Deleted row ids
	tbles = append(tbles, ex.ch.Database.GetTable(stmt.TableName.Value))

	// Check if there are any tables
	if len(tbles) == 0 {
		return nil, nil, errors.New("no tables")
	} // You can't do this!!

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	results, err := ex.filter(tbles, stmt.WhereClause, nil, true, &rowIds, nil)
	if err != nil {
		return nil, nil, err
	}

	rowsAffected := len(results)

	rA := map[string]interface{}{"RowsAffected": rowsAffected}
	results = []map[string]interface{}{rA}

	// Now we format the results
	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil, nil, nil

}

// executeUpdateStmt
func (ex *Executor) executeUpdateStmt(stmt *parser.UpdateStmt) ([]int64, []map[string]interface{}, error) {
	var rowIds []int64                // Updated row ids
	var rows []map[string]interface{} // Rows before update

	var tbles []*catalog.Table // Table list

	tbles = append(tbles, ex.ch.Database.GetTable(stmt.TableName.Value))

	// Check if there are any tables
	if len(tbles) == 0 {
		return nil, nil, errors.New("no tables")
	} // You can't do this!!

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	results, err := ex.filter(tbles, stmt.WhereClause, &stmt.SetClause, false, &rowIds, &rows)
	if err != nil {
		return nil, nil, err
	}

	rowsAffected := len(results)
	rA := map[string]interface{}{"RowsAffected": rowsAffected}
	results = []map[string]interface{}{rA}

	// Now we format the results
	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil, nil, nil

}

// GetResultSet returns the result set buffer
func (ex *Executor) GetResultSet() []byte {
	return ex.resultSetBuffer
}

// Clear clears the result set buffer
func (ex *Executor) Clear() {
	ex.resultSetBuffer = nil
}

// executeSelectStmt executes a select statement
func (ex *Executor) executeSelectStmt(stmt *parser.SelectStmt, subquery bool) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	if stmt.SelectList == nil {
		return nil, errors.New("no select list")
	}

	if stmt.SelectList != nil && stmt.TableExpression == nil {
		for _, expr := range stmt.SelectList.Expressions {
			switch expr := expr.Value.(type) {
			case *parser.Literal:
				results = append(results, map[string]interface{}{fmt.Sprintf("%v", expr.Value): expr.Value})
			case *parser.Identifier:
				results = append(results, map[string]interface{}{fmt.Sprintf("%v", expr.Value): expr.Value})
			case *parser.BinaryExpression:
				var val interface{}
				err := evaluateBinaryExpression(expr, &val)
				if err != nil {
					return nil, err
				}

				results = append(results, map[string]interface{}{fmt.Sprintf("%v", val): val})
			}
		}

	}

	var tbles []*catalog.Table // Table list

	// Check if table expression is not nil,
	// if so we need to evaluate the from clause
	// Gathering the proposed tables
	if stmt.TableExpression != nil {
		if stmt.TableExpression.FromClause == nil {
			return nil, errors.New("no from clause")
		}

		for _, tblExpr := range stmt.TableExpression.FromClause.Tables {
			tbl := ex.ch.Database.GetTable(tblExpr.Name.Value)
			if tbl == nil {
				return nil, errors.New("table does not exist")
			}

			tbles = append(tbles, tbl)
		}

	}

	// Check if there are any tables
	if len(tbles) == 0 {
		return nil, errors.New("no tables")
	} // You can't do this!!

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	rows, err := ex.filter(tbles, stmt.TableExpression.WhereClause, nil, false, nil, nil)
	if err != nil {
		return nil, err
	} // This one functions gathers the rows based on where clause.
	// Handles joins, and other conditions such as subqueries

	results = rows

	// Check for group by
	if stmt.TableExpression.GroupByClause != nil {
		grouped, err := ex.group(results, stmt.TableExpression.GroupByClause)
		if err != nil {
			return nil, err
		}

		// Check for having clause
		if stmt.TableExpression.HavingClause != nil {
			results, err = ex.having(grouped, stmt.TableExpression.HavingClause)
			if err != nil {
				return nil, err
			}

		}
	} else {

		// Based on projection (select list), we can filter the columns
		results, err = ex.selectListFilter(rows, stmt.SelectList)
		if err != nil {
			return nil, err

		}
	}

	// Check for order by
	if stmt.TableExpression.OrderByClause != nil {
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
	if subquery {
		return results, nil
	}

	// Now we format the results
	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil, nil
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

				ok, _ := ex.evaluatePredicate(having.SearchCondition.(*parser.ComparisonPredicate), map[string]interface{}{
					"COUNT": count,
				}, nil)
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

				ok, _ := ex.evaluatePredicate(newComparisonPredicate, map[string]interface{}{
					"SUM": sum,
				}, nil)
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

				ok, _ := ex.evaluatePredicate(having.SearchCondition.(*parser.ComparisonPredicate), map[string]interface{}{
					"AVG": avg,
				}, nil)
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

				ok, _ := ex.evaluatePredicate(having.SearchCondition.(*parser.ComparisonPredicate), map[string]interface{}{
					"MAX": mx,
				}, nil)
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

				ok, _ := ex.evaluatePredicate(having.SearchCondition.(*parser.ComparisonPredicate), map[string]interface{}{
					"MIN": mn,
				}, nil)
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

func (ex *Executor) selectListFilter(results []map[string]interface{}, selectList *parser.SelectList) ([]map[string]interface{}, error) {

	if selectList == nil {
		return nil, errors.New("no select list")
	}

	if len(selectList.Expressions) == 0 {
		return nil, errors.New("no select list")
	}

	columns := []string{}

	for _, expr := range selectList.Expressions {

		switch expr := expr.Value.(type) {
		case *parser.Wildcard:
			return results, nil
		case *parser.ColumnSpecification:
			columns = append(columns, expr.ColumnName.Value)
		case *parser.AggregateFunc:
			switch expr.FuncName {
			case "COUNT":
				count := len(results)
				// For count we truncate the results to one row
				results = []map[string]interface{}{map[string]interface{}{"COUNT": count}}
				columns = []string{"COUNT"}
			case "SUM":
				// Sum the values
				var sum int

				for _, row := range results {
					for _, arg := range expr.Args {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := row[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
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

				results = []map[string]interface{}{map[string]interface{}{"SUM": sum}}
				columns = []string{"SUM"}

			case "AVG":
				// Average the values
				var sum int
				var count int

				for _, row := range results {
					for _, arg := range expr.Args {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := row[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
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

				count = len(results)

				avg := sum / count

				results = []map[string]interface{}{map[string]interface{}{"AVG": avg}}
				columns = []string{"AVG"}

			case "MAX":
				// Find the maximum value
				var mx int

				for _, row := range results {
					for _, arg := range expr.Args {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := row[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
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

				results = []map[string]interface{}{map[string]interface{}{"MAX": mx}}
				columns = []string{"MAX"}

			case "MIN":
				// Find the minimum value
				var mn int
				mn = int(^uint(0) >> 1)

				for _, row := range results {
					for _, arg := range expr.Args {
						switch arg := arg.(type) {
						case *parser.ColumnSpecification:
							if _, ok := row[arg.ColumnName.Value]; !ok {
								return nil, errors.New("column does not exist")
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

				results = []map[string]interface{}{map[string]interface{}{"MIN": mn}}
				columns = []string{"MIN"}
			}
		}

	}

	for _, row := range results {
		for k, _ := range row {
			if !slices.Contains(columns, k) {
				delete(row, k)
			}
		}

	}

	return results, nil

}

// filter filters the tables
func (ex *Executor) filter(tbls []*catalog.Table, where *parser.WhereClause, update *[]*parser.SetClause, del bool, rowIds *[]int64, before *[]map[string]interface{}) ([]map[string]interface{}, error) {
	var filteredRows []map[string]interface{}

	var tbl *catalog.Table // The first table in from clause, the left table
	// Every other table is the right table

	if len(tbls) == 0 {
		return nil, errors.New("no tables")
	} else {

		tbl = tbls[0] // Set left table
	}

	var leftCond, rightCond interface{}
	var logicalOp parser.LogicalOperator

	var leftTblName *parser.Identifier

	if where == nil {
		// If there is no where clause, we return all rows
		iter := tbl.NewIterator()
		for iter.Valid() {
			row, err := iter.Next()
			if err != nil {
				continue
			}

			filteredRows = append(filteredRows, row)

		}

		return filteredRows, nil

	}
	// Check if search condition is a logical condition
	if _, ok := where.SearchCondition.(*parser.LogicalCondition); ok {
		// If so we grab the left and right conditions
		leftCond = where.SearchCondition.(*parser.LogicalCondition).Left

		rightCond = where.SearchCondition.(*parser.LogicalCondition).Right

		logicalOp = where.SearchCondition.(*parser.LogicalCondition).Op

	} else {
		leftCond = where.SearchCondition
	}

	var left interface{}
	// if left is a binary expression

	var binaryExpr *parser.BinaryExpression // can be nil

	switch leftCond.(type) {
	case *parser.ExistsPredicate:

		// Evaluate subquery
		res, err := ex.executeSelectStmt(leftCond.(*parser.ExistsPredicate).Expr.Value.(*parser.SelectStmt), true)
		if err != nil {
			return nil, err

		}

		if len(res) > 0 {
			filteredRows = res
		}

		return filteredRows, nil

	case *parser.BetweenPredicate:
		if _, ok := leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression); ok {
			left = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

		switch leftCond.(*parser.BetweenPredicate).Left.Value.(type) {
		case *parser.ColumnSpecification:
			left = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		case *parser.Literal:
			left = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.Literal).Value
		case *parser.BinaryExpression:

			binaryExpr = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression)

			// look for left table
			if _, ok := leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
				leftTblName = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
			}

			left = leftCond.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		default:
			return nil, errors.New("unsupported search condition")
		}
	case *parser.NotExpr:
		switch leftCond.(*parser.NotExpr).Expr.(type) {
		case *parser.BetweenPredicate:
			if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression); ok {
				left = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}

			switch leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(type) {
			case *parser.ColumnSpecification:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
			case *parser.Literal:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.Literal).Value
			case *parser.BinaryExpression:

				binaryExpr = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression)

				// look for left table
				if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
					leftTblName = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
				}

				left = leftCond.(*parser.NotExpr).Expr.(*parser.BetweenPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}
		case *parser.InPredicate:
			if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression); ok {
				left = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}

			switch leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(type) {
			case *parser.ColumnSpecification:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
			case *parser.Literal:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.Literal).Value
			case *parser.BinaryExpression:

				binaryExpr = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression)

				// look for left table
				if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
					leftTblName = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
				}

				left = leftCond.(*parser.NotExpr).Expr.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}

		case *parser.LikePredicate:

			if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression); ok {
				left = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}

			switch leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(type) {
			case *parser.ColumnSpecification:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
			case *parser.Literal:
				left = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.Literal).Value
			case *parser.BinaryExpression:

				binaryExpr = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression)

				// look for left table
				if _, ok := leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
					leftTblName = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
				}

				left = leftCond.(*parser.NotExpr).Expr.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
			}
		}
	case *parser.ComparisonPredicate:

		if _, ok := leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression); ok {
			left = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

		switch leftCond.(*parser.ComparisonPredicate).Left.Value.(type) {
		case *parser.ColumnSpecification:
			left = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		case *parser.Literal:
			left = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.Literal).Value
		case *parser.BinaryExpression:

			binaryExpr = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression)

			// look for left table
			if _, ok := leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
				leftTblName = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
			}

			left = leftCond.(*parser.ComparisonPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		default:
			return nil, errors.New("unsupported search condition")
		}
	case *parser.IsPredicate:

		if _, ok := leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression); ok {
			left = leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

		switch leftCond.(*parser.IsPredicate).Left.Value.(type) {
		case *parser.ColumnSpecification:
			left = leftCond.(*parser.IsPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		case *parser.Literal:
			left = leftCond.(*parser.IsPredicate).Left.Value.(*parser.Literal).Value
		case *parser.BinaryExpression:

			binaryExpr = leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression)

			// look for left table
			if _, ok := leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
				leftTblName = leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
			}

			left = leftCond.(*parser.IsPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value

		}

	case *parser.LikePredicate:

		if _, ok := leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression); ok {
			left = leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

		switch leftCond.(*parser.LikePredicate).Left.Value.(type) {
		case *parser.ColumnSpecification:
			left = leftCond.(*parser.LikePredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		case *parser.Literal:
			left = leftCond.(*parser.LikePredicate).Left.Value.(*parser.Literal).Value
		case *parser.BinaryExpression:

			binaryExpr = leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression)

			// look for left table
			if _, ok := leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
				leftTblName = leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
			}

			left = leftCond.(*parser.LikePredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

	case *parser.InPredicate:

		if _, ok := leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression); ok {
			left = leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}

		switch leftCond.(*parser.InPredicate).Left.Value.(type) {
		case *parser.ColumnSpecification:
			left = leftCond.(*parser.InPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		case *parser.Literal:
			left = leftCond.(*parser.InPredicate).Left.Value.(*parser.Literal).Value
		case *parser.BinaryExpression:

			binaryExpr = leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression)

			// look for left table
			if _, ok := leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); ok {
				leftTblName = leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName
			}

			left = leftCond.(*parser.InPredicate).Left.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value
		}
	}

	var row map[string]interface{}
	var err error

	// Check if tbl is indexed

	var idx *catalog.Index

	idx = tbl.CheckIndexedColumn(left.(string), true)
	if idx == nil {
		// try not unique index
		idx = tbl.CheckIndexedColumn(left.(string), false)
		if idx != nil {
			idx = nil

		}

	}

	if idx != nil {

		keys, err := idx.GetBtree().InOrderTraversal()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			for _, val := range key.V {
				int64Str := string(val)

				rowId, err := strconv.ParseInt(int64Str, 10, 64)
				if err != nil {
					return nil, err
				}

				row, err = tbl.GetRow(rowId)
				if err != nil {
					return nil, err
				}

				err = ex.evaluateFinalCondition(where, &filteredRows, rightCond, leftCond, leftTblName, logicalOp, left, binaryExpr, row, tbls, nil, rowId, false, rowIds, before)
				if err != nil {
					return nil, err
				}

			}

		}

	} else {

		iter := tbl.NewIterator()

		if update == nil && !del {

			for iter.Valid() {
				row, err = iter.Next()
				if err != nil {
					continue
				}

				err = ex.evaluateFinalCondition(where, &filteredRows, rightCond, leftCond, leftTblName, logicalOp, left, binaryExpr, row, tbls, update, iter.Current(), del, rowIds, before)
				if err != nil {
					return nil, err
				}

				if !iter.Valid() {
					break
				}

			}
		} else {
			for iter.ValidUpdateIter() {
				row, err = iter.Next()
				if err != nil {
					continue
				}

				err = ex.evaluateFinalCondition(where, &filteredRows, rightCond, leftCond, leftTblName, logicalOp, left, binaryExpr, row, tbls, update, iter.Current()-1, del, rowIds, before)
				if err != nil {
					return nil, err
				}

				if !iter.ValidUpdateIter() {
					break
				}

			}
		}

	}

	return filteredRows, nil
}

// evaluateFinalCondition evaluates the final condition
func (ex *Executor) evaluateFinalCondition(where *parser.WhereClause, filteredRows *[]map[string]interface{}, rightCond, leftCond interface{}, leftTblName *parser.Identifier, logicalOp parser.LogicalOperator, left interface{}, binaryExpr *parser.BinaryExpression, row map[string]interface{}, tbls []*catalog.Table, update *[]*parser.SetClause, rowId int64, del bool, rowIds *[]int64, before *[]map[string]interface{}) error {
	var err error
	if binaryExpr != nil {
		var val interface{}

		// Replace binary expression column spec with a literal
		binaryExpr.Left = &parser.Literal{Value: row[left.(string)]}

		err = evaluateBinaryExpression(binaryExpr, &val)
		if err != nil {
			return err
		}

		row[left.(string)] = val
	}

	if logicalOp == parser.OP_AND {
		ok, res := ex.evaluatePredicate(leftCond, row, tbls)
		if ok {
			ok, _ := ex.evaluatePredicate(rightCond, row, tbls)
			if ok {

				var resTbls []string

				for t, _ := range res {
					resTbls = append(resTbls, t)

				}

				if len(res) == 1 {
					for _, rows := range res[resTbls[0]] {
						*filteredRows = append(*filteredRows, rows)

					}
				} else if len(res) > 1 {
					newRow := map[string]interface{}{}
					for _, tblName := range resTbls {
						for _, rows := range res[tblName] {
							for k, v := range rows {
								newRow[k] = v //newRow[fmt.Sprintf("%s.%s", tblName, k)] = v
							}

						}

					}

					*filteredRows = append(*filteredRows, newRow)
				}

			}
		}
	} else if logicalOp == parser.OP_OR {
		ok, res := ex.evaluatePredicate(leftCond, row, tbls)
		if ok {

			var resTbls []string

			for t, _ := range res {
				resTbls = append(resTbls, t)

			}

			if len(res) == 1 {
				for _, rows := range res[resTbls[0]] {
					*filteredRows = append(*filteredRows, rows)

				}
			} else if len(res) > 1 {

				newRow := map[string]interface{}{}
				for _, tblName := range resTbls {
					for _, rows := range res[tblName] {
						for k, v := range rows {
							newRow[k] = v
						}

					}

				}

				*filteredRows = append(*filteredRows, newRow)
			}
		}

		ok, res = ex.evaluatePredicate(rightCond, row, tbls)
		if ok {

			var resTbls []string

			for t, _ := range res {
				resTbls = append(resTbls, t)

			}

			if len(res) == 1 {
				for _, r := range res[resTbls[0]] {
					// copy other columns from the row if they dont exist in current rows
					newRow := r

					if len(*filteredRows) > 0 {
						for k, _ := range (*filteredRows)[0] {
							if _, ok = newRow[k]; !ok {
								newRow[k] = nil
							}
						}

						*filteredRows = append(*filteredRows, newRow)
					}

				}
			} else if len(res) > 1 {
				newRow := map[string]interface{}{}
				for _, tblName := range resTbls {
					for _, rows := range res[tblName] {
						for k, v := range rows {
							newRow[k] = v
						}

					}

				}

				*filteredRows = append(*filteredRows, newRow)
			}
		}

	} else {
		ok, res := ex.evaluatePredicate(where.SearchCondition, row, tbls)
		if ok {

			var resTbls []string

			for t, _ := range res {
				resTbls = append(resTbls, t)

			}

			if len(res) == 1 {
				for _, r := range res[resTbls[0]] {
					if rowIds != nil || before != nil {

						if update != nil {
							*rowIds = append(*rowIds, rowId)
							*before = append(*before, row)

							err := tbls[0].UpdateRow(rowId, row, convertSetClauseToCatalogLike(update))
							if err != nil {
								return err
							}
						} else if del {
							*rowIds = append(*rowIds, rowId)
							err := tbls[0].DeleteRow(rowId)
							if err != nil {
								return err
							}
						}
					}

					*filteredRows = append(*filteredRows, r)
				}
			} else if len(res) > 1 {

				newRow := map[string]interface{}{}
				for _, tblName := range resTbls {
					for _, r := range res[tblName] {

						if rowIds != nil || before != nil {
							if update != nil {
								*rowIds = append(*rowIds, rowId)
								*before = append(*before, row)

								err := tbls[0].UpdateRow(rowId, row, convertSetClauseToCatalogLike(update))
								if err != nil {
									return err
								}
							} else if del {
								*rowIds = append(*rowIds, rowId)

								err := tbls[0].DeleteRow(rowId)
								if err != nil {
									return err
								}
							}
						}

						for k, v := range r {
							if leftTblName != nil {
								if len(strings.Split(k, ".")) == 1 {
									newRow[fmt.Sprintf("%s.%s", leftTblName.Value, k)] = v
								} else {
									newRow[k] = v
								}
							} else {
								newRow[k] = v
							}
						}

					}

				}

				*filteredRows = append(*filteredRows, newRow)
			}

		}
	}

	return nil

}

// evaluatePredicate evaluates a predicate condition on a row
func (ex *Executor) evaluatePredicate(cond interface{}, row map[string]interface{}, tbls []*catalog.Table) (bool, map[string][]map[string]interface{}) {
	results := make(map[string][]map[string]interface{})

	_, isNot := cond.(*parser.NotExpr)
	if isNot {
		cond = cond.(*parser.NotExpr).Expr

	}

	var left interface{}

	switch cond := cond.(type) {
	case *parser.BetweenPredicate:

		if _, ok := cond.Left.Value.(*parser.ColumnSpecification); ok {
			left = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
		}

		if _, ok := cond.Left.Value.(*parser.BinaryExpression); ok {
			var val interface{}
			err := evaluateBinaryExpression(cond.Left.Value.(*parser.BinaryExpression), &val)
			if err != nil {
				return false, nil
			}

			left = val
		}

		if _, ok := cond.Left.Value.(*parser.Literal); ok {
			left = cond.Left.Value.(*parser.Literal).Value
		}

		for k, _ := range row {
			// convert columnname to table.columnname
			if len(strings.Split(k, ".")) == 1 {
				row[fmt.Sprintf("%s.%s", tbls[0].Name, k)] = row[k]
				delete(row, k)
			}
		}

		if !isNot {

			if left.(int) >= int(cond.Lower.Value.(*parser.Literal).Value.(uint64)) && left.(int) <= int(cond.Upper.Value.(*parser.Literal).Value.(uint64)) {
				results[tbls[0].Name] = []map[string]interface{}{row}
			}
		} else {
			if left.(int) < int(cond.Lower.Value.(*parser.Literal).Value.(uint64)) || left.(int) > int(cond.Upper.Value.(*parser.Literal).Value.(uint64)) {
				results[tbls[0].Name] = []map[string]interface{}{row}
			}
		}

		if len(results) > 0 {
			return true, results
		}

	case *parser.IsPredicate:

		if _, ok := cond.Left.Value.(*parser.ColumnSpecification); ok {
			left = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
		}

		if _, ok := cond.Left.Value.(*parser.BinaryExpression); ok {
			var val interface{}
			err := evaluateBinaryExpression(cond.Left.Value.(*parser.BinaryExpression), &val)
			if err != nil {
				return false, nil
			}

			left = val
		}

		if _, ok := cond.Left.Value.(*parser.Literal); ok {
			left = cond.Left.Value.(*parser.Literal).Value
		}

		for k, _ := range row {
			// convert columnname to table.columnname
			if len(strings.Split(k, ".")) == 1 {
				row[fmt.Sprintf("%s.%s", tbls[0].Name, k)] = row[k]
				delete(row, k)
			}
		}

		if cond.Null {
			if left == nil {
				results[tbls[0].Name] = []map[string]interface{}{row}
			}
		} else {

			if left != nil {
				results[tbls[0].Name] = []map[string]interface{}{row}
			}
		}

		if len(results) > 0 {
			return true, results
		}

	case *parser.LikePredicate:

		if _, ok := cond.Left.Value.(*parser.ColumnSpecification); ok {
			left = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
		}

		if _, ok := cond.Left.Value.(*parser.BinaryExpression); ok {
			var val interface{}
			err := evaluateBinaryExpression(cond.Left.Value.(*parser.BinaryExpression), &val)
			if err != nil {
				return false, nil
			}

			left = val
		}

		if _, ok := cond.Left.Value.(*parser.Literal); ok {
			left = cond.Left.Value.(*parser.Literal).Value
		}

		for k, _ := range row {
			// convert columnname to table.columnname
			if len(strings.Split(k, ".")) == 1 {
				row[fmt.Sprintf("%s.%s", tbls[0].Name, k)] = row[k]
				delete(row, k)
			}
		}

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

		// check if left is a string
		if _, ok := left.(string); ok {

			pattern := cond.Pattern.Value

			if !isNot {

				switch {
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%") && strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// '%a%'
					if strings.Contains(left.(string), strings.TrimPrefix(strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'"), "'%")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// 'a%'
					if strings.HasPrefix(left.(string), strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%"):
					// '%a'
					if strings.HasSuffix(left.(string), strings.TrimPrefix(pattern.(*parser.Literal).Value.(string), "'%")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case len(strings.Split(pattern.(*parser.Literal).Value.(string), "%")) == 2:
					// 'a%b'
					lStr := strings.TrimLeft(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[0], "'")
					rStr := strings.TrimRight(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[1], "'")

					if strings.HasPrefix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), lStr) && strings.HasSuffix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), rStr) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}

				default:
					return false, nil

				}
			} else {
				switch {
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%") && strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// '%a%'
					if !strings.Contains(left.(string), strings.TrimPrefix(strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'"), "'%")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case strings.HasSuffix(pattern.(*parser.Literal).Value.(string), "%'"):
					// 'a%'
					if !strings.HasPrefix(left.(string), strings.TrimSuffix(pattern.(*parser.Literal).Value.(string), "%'")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case strings.HasPrefix(pattern.(*parser.Literal).Value.(string), "'%"):
					// '%a'
					if !strings.HasSuffix(left.(string), strings.TrimPrefix(pattern.(*parser.Literal).Value.(string), "'%")) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}
				case len(strings.Split(pattern.(*parser.Literal).Value.(string), "%")) == 2:
					// 'a%b'
					lStr := strings.TrimLeft(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[0], "'")
					rStr := strings.TrimRight(strings.Split(pattern.(*parser.Literal).Value.(string), "%")[1], "'")

					if !strings.HasPrefix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), lStr) && !strings.HasSuffix(strings.TrimPrefix(strings.TrimSuffix(left.(string), "'"), "'"), rStr) {
						results[tbls[0].Name] = []map[string]interface{}{row}
					}

				default:
					return false, nil

				}
			}

		} else {
			return false, nil

		}

		if len(results) > 0 {
			return true, results
		}

	case *parser.InPredicate:

		var leftCol string

		if _, ok := cond.Left.Value.(*parser.ColumnSpecification); ok {
			left = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
			leftCol = cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value
		}

		if _, ok := cond.Left.Value.(*parser.BinaryExpression); ok {
			var val interface{}
			err := evaluateBinaryExpression(cond.Left.Value.(*parser.BinaryExpression), &val)
			if err != nil {
				return false, nil
			}

			left = val
		}

		if _, ok := cond.Left.Value.(*parser.Literal); ok {
			left = cond.Left.Value.(*parser.Literal).Value
		}

		for k, _ := range row {
			// convert columnname to table.columnname
			if len(strings.Split(k, ".")) == 1 {
				row[fmt.Sprintf("%s.%s", tbls[0].Name, k)] = row[k]
				delete(row, k)
			}
		}

		// Check if val.Value.(*parser.Literal).Value is a select statement
		if _, ok := cond.Values[0].Value.(*parser.SelectStmt); ok {
			// Run the select statement
			stmt := cond.Values[0].Value.(*parser.SelectStmt)

			res, err := ex.executeSelectStmt(stmt, true)
			if err != nil {
				return false, nil
			}

			if !isNot {

				for _, r := range res {
					switch left.(type) {
					case int:
						left = int(left.(int))
						if left == r[leftCol].(int) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case uint64:
						if left.(uint64) == r[leftCol].(uint64) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}
					case float64:
						if left.(float64) == r[leftCol].(float64) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case string:
						if left.(string) == r[leftCol].(string) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}

					}
				}
			} else {
				for _, r := range res {
					switch left.(type) {
					case int:
						left = int(left.(int))
						if left != r[leftCol].(int) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case uint64:
						if left.(uint64) != r[leftCol].(uint64) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}
					case float64:
						if left.(float64) != r[leftCol].(float64) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case string:
						if left.(string) != r[leftCol].(string) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}

					}
				}
			}
		} else {

			if !isNot {

				for _, val := range cond.Values {
					switch left.(type) {
					case int:
						left = int(left.(int))
						if left == int(val.Value.(*parser.Literal).Value.(uint64)) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case uint64:
						if left.(uint64) == val.Value.(*parser.Literal).Value.(uint64) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}
					case float64:
						if left.(float64) == val.Value.(*parser.Literal).Value.(float64) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case string:
						if left.(string) == val.Value.(*parser.Literal).Value.(string) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}

					}
				}

			} else {
				for _, val := range cond.Values {
					switch left.(type) {
					case int:
						left = int(left.(int))
						if left != int(val.Value.(*parser.Literal).Value.(uint64)) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case uint64:
						if left.(uint64) != val.Value.(*parser.Literal).Value.(uint64) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}
					case float64:
						if left.(float64) != val.Value.(*parser.Literal).Value.(float64) {
							results[tbls[0].Name] = []map[string]interface{}{row}
						}
					case string:
						if left.(string) != val.Value.(*parser.Literal).Value.(string) {
							results[tbls[0].Name] = []map[string]interface{}{row}

						}

					}
				}
			}
		}

		if len(results) > 0 {
			return true, results
		}

	case *parser.ComparisonPredicate: // Joins are only supported with comparison predicates

		var right interface{}
		var ok bool

		if _, ok = cond.Left.Value.(*parser.ColumnSpecification); ok {

			left, ok = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
			if !ok {
				return false, nil
			}

			if cond.Left.Value.(*parser.ColumnSpecification).TableName != nil {

				results[cond.Left.Value.(*parser.ColumnSpecification).TableName.Value] = []map[string]interface{}{row}

				newRow := map[string]interface{}{}

				for k, v := range row {
					newRow[fmt.Sprintf("%s.%s", tbls[0].Name, k)] = v
				}

				results[tbls[0].Name] = []map[string]interface{}{newRow}

			} else {

				results[tbls[0].Name] = []map[string]interface{}{row}
			}
		} else if _, ok = cond.Left.Value.(*parser.BinaryExpression); ok {

			var val interface{}
			err := evaluateBinaryExpression(cond.Left.Value.(*parser.BinaryExpression), &val)
			if err != nil {
				return false, nil
			}

			left = val

			results[tbls[0].Name] = []map[string]interface{}{row}
		} else if _, ok = cond.Left.Value.(*parser.Literal); ok {
			left = cond.Left.Value.(*parser.Literal).Value
		}

		if _, ok = cond.Right.Value.(*parser.Literal); ok {
			right = cond.Right.Value.(*parser.Literal).Value
		} else if _, ok = cond.Right.Value.(*parser.ColumnSpecification); ok {
			tblName := cond.Right.Value.(*parser.ColumnSpecification).TableName.Value
			colName := cond.Right.Value.(*parser.ColumnSpecification).ColumnName.Value

			for _, tbl := range tbls {
				if tbl.Name == tblName {

					rightRow, err := ex.filter([]*catalog.Table{tbl},
						&parser.WhereClause{
							SearchCondition: &parser.ComparisonPredicate{
								Left: &parser.ValueExpression{Value: &parser.ColumnSpecification{
									TableName:  &parser.Identifier{Value: tblName},
									ColumnName: &parser.Identifier{Value: colName}},
								}, Right: &parser.ValueExpression{Value: &parser.Literal{Value: row[colName]}}, Op: cond.Op}}, nil, false, nil, nil)
					if err != nil {
						return false, nil
					}

					if len(rightRow) == 0 {
						return false, nil
					}

					right = rightRow[0][colName]

					if right == nil {
						right = rightRow[0][fmt.Sprintf("%s.%s", tblName, colName)]
					}

					results[tbl.Name] = rightRow
				}
			}
		} else if _, ok = cond.Right.Value.(*parser.BinaryExpression); ok {
			binaryExpr := cond.Right.Value.(*parser.BinaryExpression)

			var val interface{}

			// left should be a column

			if _, ok = cond.Right.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification); !ok {
				return false, nil
			}

			tblName := cond.Right.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).TableName.Value
			colName := cond.Right.Value.(*parser.BinaryExpression).Left.(*parser.ColumnSpecification).ColumnName.Value

			for _, tbl := range tbls {
				if tbl.Name == tblName {
					rightRow, err := ex.filter([]*catalog.Table{tbl},
						&parser.WhereClause{
							SearchCondition: &parser.ComparisonPredicate{
								Left: &parser.ValueExpression{Value: &parser.ColumnSpecification{
									TableName:  &parser.Identifier{Value: tblName},
									ColumnName: &parser.Identifier{Value: colName}},
								}, Right: &parser.ValueExpression{Value: &parser.Literal{Value: row[colName]}}, Op: cond.Op}}, nil, false, nil, nil)
					if err != nil {
						return false, nil
					}

					binaryExpr.Left = &parser.Literal{Value: rightRow[0][colName]}

				}
			}

			err := evaluateBinaryExpression(binaryExpr, &val)
			if err != nil {
				return false, nil
			}

			right = val

			results[tbls[0].Name] = []map[string]interface{}{row}

		}

		// check if right is not a parser.Literal
		if _, ok := cond.Right.Value.(*parser.Literal); !ok {
			if _, ok := cond.Right.Value.(*parser.ColumnSpecification); !ok {
				// check for subquery
				if _, ok := cond.Right.Value.(*parser.ValueExpression).Value.(*parser.SelectStmt); ok {
					res, err := ex.executeSelectStmt(cond.Right.Value.(*parser.ValueExpression).Value.(*parser.SelectStmt), true)
					if err != nil {
						return false, nil
					}

					// if results are greater than one only get first row first column
					for _, r := range res {
						for _, v := range r {
							right = v
							break
						}
					}
				}
			}
		}

		switch left.(type) {
		case int:
			left = int(left.(int))
		case uint64:
			left = int(left.(uint64))
		}

		switch right.(type) {
		case int:
			right = int(right.(int))
		case uint64:
			right = int(right.(uint64))

		}

		// convert left and right to float64
		if _, ok := left.(string); !ok {
			if _, ok := left.(float64); !ok {
				left = float64(left.(int))
			}

			if _, ok := right.(int); !ok {
				if _, ok := right.(float64); !ok {
					// check if right is string
					if _, ok := right.(string); ok {
						return false, nil
					}

					right = float64(right.(int))
				}
			} else {
				right = float64(right.(int))
			}

		}

		// The right type should be the same as the left type in the end

		switch cond.Op {
		case parser.OP_EQ:
			switch left.(type) {
			case int:
				return left.(int) == right.(int), results

			case float64:
				return left.(float64) == right.(float64), results
			case string:
				return left.(string) == right.(string), results
			}
		case parser.OP_NEQ:
			return left != right, results
		case parser.OP_LT:
			return left.(float64) < right.(float64), results
		case parser.OP_LTE:
			return left.(float64) <= right.(float64), results
		case parser.OP_GT:
			return left.(float64) > right.(float64), results
		case parser.OP_GTE:
			return left.(float64) >= right.(float64), results
		}

	}

	return false, nil

}

// evaluateBinaryExpression evaluates a binary expression
func evaluateBinaryExpression(expr *parser.BinaryExpression, val *interface{}) error {
	leftInt, ok := expr.Left.(*parser.Literal).Value.(uint64)
	if !ok {
		_, ok = expr.Left.(*parser.Literal).Value.(int)
		if !ok {
			return fmt.Errorf("left value is not a number")
		}

		leftInt = uint64(expr.Left.(*parser.Literal).Value.(int))
	}

	left := float64(leftInt)

	var right interface{}
	if _, ok := expr.Right.(*parser.BinaryExpression); ok {
		err := evaluateBinaryExpression(expr.Right.(*parser.BinaryExpression), &right)
		if err != nil {
			return err
		}
	} else {
		rightInt, ok := expr.Right.(*parser.Literal).Value.(uint64)
		if !ok {
			_, ok = expr.Right.(*parser.Literal).Value.(int)
			if !ok {
				return fmt.Errorf("right value is not a number")
			}

			rightInt = uint64(expr.Left.(*parser.Literal).Value.(int))
		}

		right = float64(rightInt)

	}

	switch expr.Op {
	case parser.OP_PLUS:
		*val = left + right.(float64)
	case parser.OP_MINUS:
		*val = left - right.(float64)
	case parser.OP_MULT:
		*val = left * right.(float64)
	case parser.OP_DIV:
		*val = left / right.(float64)

	}

	return nil
}

// convertSetClauseToCatalogLike converts a set clause(s) to a catalog set clause(s)
func convertSetClauseToCatalogLike(setClause *[]*parser.SetClause) []*catalog.SetClause {
	var setClauses []*catalog.SetClause

	for _, set := range *setClause {
		setClauses = append(setClauses, &catalog.SetClause{
			ColumnName: set.Column.Value,
			Value:      set.Value.Value,
		})
	}

	return setClauses

}
