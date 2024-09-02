package executor

import (
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/parser"
	"ariasql/shared"
	"errors"
	"fmt"
	"strconv"
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
		return ex.aria.Catalog.CreateDatabase(stmt.Name.Value)
	case *parser.CreateTableStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		err := ex.ch.Database.CreateTable(stmt.TableName.Value, stmt.TableSchema)
		if err != nil {
			return err
		}

		return nil

	case *parser.DropTableStmt:
		if ex.ch.Database == nil {
			return errors.New("no database selected")
		}

		err := ex.ch.Database.DropTable(stmt.TableName.Value)
		if err != nil {
			return err
		}

		return nil
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

		return nil
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

		return nil
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

		return nil
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

		return nil

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

	tbles := []*catalog.Table{} // Table list

	if stmt.TableExpression != nil {
		if stmt.TableExpression.FromClause == nil {
			return errors.New("no from clause")
		}

		for _, tbl := range stmt.TableExpression.FromClause.Tables {
			tbl := ex.ch.Database.GetTable(tbl.Name.Value)
			if tbl == nil {
				return errors.New("table does not exist")
			}

			tbles = append(tbles, tbl)
		}

	}

	// Check if there are any tables
	if len(tbles) == 0 {
		return errors.New("no tables")
	}

	// If there are more than one table, join them
	if len(tbles) > 1 {
		// Join the tables
		//joinedTable := shared.JoinTables(tbles)
		//if stmt.TableExpression.WhereClause != nil {
		//	// Filter the results
		//	joinedTable = shared.FilterTable(joinedTable, stmt.TableExpression.WhereClause.SearchCondition)
		//}
		//
		//// Get the results
		//results = shared.GetResults(joinedTable, stmt.SelectList)
	} else {
		// For a 1 table query we can evaluate the search condition
		// If the column is indexed, we can use the index to locate rows faster

		if stmt.TableExpression.WhereClause != nil {
			// Filter the results
			rows, err := filter(tbles[0], stmt.TableExpression.WhereClause)
			if err != nil {
				return err
			}

			results = rows
		}

	}

	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil
}

func filter(tbl *catalog.Table, where *parser.WhereClause) ([]map[string]interface{}, error) {
	var filteredRows []map[string]interface{}

	// In a search condition the left side should be a column
	// The right side can be a column or a literal

	switch where.SearchCondition.(type) {
	case *parser.ComparisonPredicate:
		left := where.SearchCondition.(*parser.ComparisonPredicate).Left.Value.(*parser.ColumnSpecification).ColumnName.Value

		// We check for an index on the column
		idx := tbl.CheckIndexedColumn(left, tbl.TableSchema.ColumnDefinitions[left].Unique)
		if idx != nil {
			// If there is an index, we can use it
			// We can use the index to locate the rows faster

			key, err := idx.GetBtree().Get([]byte(fmt.Sprintf("%v", where.SearchCondition.(*parser.ComparisonPredicate).Right.Value.(*parser.Literal).Value)))
			if err != nil {
				return filteredRows, err
			}

			// Get the row
			for _, rowIdBytes := range key.V {
				int64Str := string(rowIdBytes)

				rowId, err := strconv.ParseInt(int64Str, 10, 64)
				if err != nil {
					return filteredRows, err
				}

				row, err := tbl.GetRow(rowId)
				if err != nil {
					return filteredRows, err
				}

				if evaluatePredicate(where.SearchCondition, row) {
					filteredRows = append(filteredRows, row)
				}

			}
		} else {
			iter := tbl.NewIterator()
			for iter.Valid() {
				row, err := iter.Next()
				if err != nil {
					break
				}

				if evaluatePredicate(where.SearchCondition, row) {
					filteredRows = append(filteredRows, row)
				}

			}

		}

	}

	return filteredRows, nil
}

// evaluatePredicate evaluates a predicate
func evaluatePredicate(cond interface{}, row map[string]interface{}) bool {
	switch cond := cond.(type) {
	case *parser.ComparisonPredicate:

		var left, right interface{}
		var ok bool

		if _, ok = cond.Left.Value.(*parser.ColumnSpecification); ok {
			left, ok = row[cond.Left.Value.(*parser.ColumnSpecification).ColumnName.Value]
			if !ok {
				return false
			}
		}

		if _, ok = cond.Right.Value.(*parser.Literal); ok {
			right = cond.Right.Value.(*parser.Literal).Value
		}

		// The right type should be the same as the left type in the end

		switch cond.Op {
		case parser.OP_EQ:
			return left == right
		case parser.OP_NEQ:
			return left != right
		case parser.OP_LT:
			return left.(float64) < right.(float64)
		case parser.OP_LTE:
			return left.(float64) <= right.(float64)
		case parser.OP_GT:
			return left.(float64) > right.(float64)
		case parser.OP_GTE:
			return left.(float64) >= right.(float64)
		}

	}

	return false

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
