package executor

import (
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/parser"
	"ariasql/shared"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

		for _, tblExpr := range stmt.TableExpression.FromClause.Tables {
			tbl := ex.ch.Database.GetTable(tblExpr.Name.Value)
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

	// For a 1 table query we can evaluate the search condition
	// If the column is indexed, we can use the index to locate rows faster

	// Filter the results
	rows, err := filter(tbles, stmt.TableExpression.WhereClause)
	if err != nil {
		return err
	}

	results = rows

	// Based on projection (select list), we can filter the columns

	ex.resultSetBuffer = shared.CreateTableByteArray(results, shared.GetHeaders(results))

	return nil
}

// filter filters the tables
func filter(tbls []*catalog.Table, where *parser.WhereClause) ([]map[string]interface{}, error) {
	var filteredRows []map[string]interface{}

	var tbl *catalog.Table // The first table in from clause, the left table
	// Every other table is the right table

	if len(tbls) == 0 {
		return nil, errors.New("no tables")
	} else {

		tbl = tbls[0]
	}

	var leftCond, rightCond interface{}
	var logicalOp parser.LogicalOperator
	var leftTblName *parser.Identifier

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

				err = evaluateFinalCondition(where, &filteredRows, rightCond, leftCond, leftTblName, logicalOp, left, binaryExpr, row, tbls)
				if err != nil {
					return nil, err
				}
			}

		}

	} else {

		iter := tbl.NewIterator()
		for iter.Valid() {
			row, err = iter.Next()
			if err != nil {
				continue
			}

			err = evaluateFinalCondition(where, &filteredRows, rightCond, leftCond, leftTblName, logicalOp, left, binaryExpr, row, tbls)
			if err != nil {
				return nil, err
			}

		}

	}

	return filteredRows, nil
}

func evaluateFinalCondition(where *parser.WhereClause, filteredRows *[]map[string]interface{}, rightCond, leftCond interface{}, leftTblName *parser.Identifier, logicalOp parser.LogicalOperator, left interface{}, binaryExpr *parser.BinaryExpression, row map[string]interface{}, tbls []*catalog.Table) error {
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
		ok, res := evaluatePredicate(leftCond, row, tbls)
		if ok {
			ok, _ := evaluatePredicate(rightCond, row, tbls)
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
		ok, res := evaluatePredicate(leftCond, row, tbls)
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

		ok, res = evaluatePredicate(rightCond, row, tbls)
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
		ok, res := evaluatePredicate(where.SearchCondition, row, tbls)
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
func evaluatePredicate(cond interface{}, row map[string]interface{}, tbls []*catalog.Table) (bool, map[string][]map[string]interface{}) {
	results := make(map[string][]map[string]interface{})

	switch cond := cond.(type) {
	case *parser.IsPredicate:

		var left interface{}

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

	case *parser.InPredicate:

		var left interface{}

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

		for _, val := range cond.Values {
			switch left.(type) {
			case int:
				left = int(left.(int))
				return left == val.Value.(int), results
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

		if len(results) > 0 {
			return true, results
		}

	case *parser.ComparisonPredicate: // Joins are only supported with comparison predicates

		var left, right interface{}
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
		}

		if _, ok = cond.Right.Value.(*parser.Literal); ok {
			right = cond.Right.Value.(*parser.Literal).Value
		} else if _, ok = cond.Right.Value.(*parser.ColumnSpecification); ok {
			tblName := cond.Right.Value.(*parser.ColumnSpecification).TableName.Value
			colName := cond.Right.Value.(*parser.ColumnSpecification).ColumnName.Value

			for _, tbl := range tbls {
				if tbl.Name == tblName {

					rightRow, err := filter([]*catalog.Table{tbl},
						&parser.WhereClause{
							SearchCondition: &parser.ComparisonPredicate{
								Left: &parser.ValueExpression{Value: &parser.ColumnSpecification{
									TableName:  &parser.Identifier{Value: tblName},
									ColumnName: &parser.Identifier{Value: colName}},
								}, Right: &parser.ValueExpression{Value: &parser.Literal{Value: row[colName]}}, Op: cond.Op}})
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
					rightRow, err := filter([]*catalog.Table{tbl},
						&parser.WhereClause{
							SearchCondition: &parser.ComparisonPredicate{
								Left: &parser.ValueExpression{Value: &parser.ColumnSpecification{
									TableName:  &parser.Identifier{Value: tblName},
									ColumnName: &parser.Identifier{Value: colName}},
								}, Right: &parser.ValueExpression{Value: &parser.Literal{Value: row[colName]}}, Op: cond.Op}})
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
