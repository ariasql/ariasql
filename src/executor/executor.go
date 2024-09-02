package executor

import (
	"ariasql/core"
	"ariasql/parser"
	"ariasql/shared"
	"errors"
	"fmt"
)

// Executor is the main executor structure
type Executor struct {
	aria            *core.AriaSQL // AriaSQL instance pointer
	ch              *core.Channel // Channel pointer
	resultSetBuffer []byte        // Result set buffer
}

// New creates a new Executor
func New(aria *core.AriaSQL, ch *core.Channel) *Executor {
	return &Executor{ch: ch, aria: aria}
}

// Execute executes a statement
func (ex *Executor) Execute(stmt parser.Statement) error {
	switch stmt := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		err := ex.aria.Catalog.CreateDatabase(stmt.Name.Value)
		if err != nil {
			return err
		}
	case *parser.CreateTableStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		err := ex.ch.Database.CreateTable(stmt.TableName.Value, stmt.TableSchema)
		if err != nil {
			return err
		}

	case *parser.DropTableStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		err := ex.ch.Database.DropTable(stmt.TableName.Value)
		if err != nil {
			return err
		}
	case *parser.CreateIndexStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		tbl := ex.ch.Database.GetTable(stmt.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		// convert *parser.Identifier to []string
		var columns []string
		for _, col := range stmt.ColumnNames {
			columns = append(columns, col.Value)
		}

		err := tbl.CreateIndex(stmt.IndexName.Value, columns, stmt.Unique)
		if err != nil {
			return err
		}
	case *parser.DropIndexStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		tbl := ex.ch.Database.GetTable(stmt.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		err := tbl.DropIndex(stmt.IndexName.Value)
		if err != nil {
			return err
		}

	case *parser.InsertStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		tbl := ex.ch.Database.GetTable(stmt.TableName.Value)
		if tbl == nil {
			return errors.New("table does not exist")
		}

		rows := []map[string]interface{}{}

		for _, row := range stmt.Values {
			data := map[string]interface{}{}
			for i, col := range stmt.ColumnNames {
				data[col.Value] = row[i].Value
			}
			rows = append(rows, data)

		}

		err := tbl.Insert(rows)
		if err != nil {
			return err
		}

	case *parser.UseStmt:
		db := ex.aria.Catalog.GetDatabase(stmt.DatabaseName.Value)
		if db == nil {
			return errors.New("database does not exist")
		}

		ex.ch.Database = db
		return nil
	case *parser.DropDatabaseStmt:
		err := ex.aria.Catalog.DropDatabase(stmt.Name.Value)
		if err != nil {
			return err
		}

	case *parser.SelectStmt:
		err := ex.executeSelectStmt(stmt)
		if err != nil {
			return err
		}

		return nil
	default:
		return errors.New("unsupported statement")

	}

	return errors.New("unsupported statement")
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
func (ex *Executor) executeSelectStmt(stmt *parser.SelectStmt) error {
	results := []map[string]interface{}{}

	if stmt.SelectList == nil {
		return errors.New("no select list")
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
					return err
				}

				results = append(results, map[string]interface{}{fmt.Sprintf("%v", val): val})
			}
		}

	}

	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil
}

// evaluateBinaryExpression evaluates a binary expression
func evaluateBinaryExpression(expr *parser.BinaryExpression, val *interface{}) error {
	leftInt, ok := expr.Left.(*parser.Literal).Value.(uint64)
	if !ok {
		return fmt.Errorf("left value is not a number")
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
			return fmt.Errorf("right value is not a number")
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
