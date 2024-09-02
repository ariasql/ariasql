package executor

import (
	"ariasql/core"
	"ariasql/parser"
	"ariasql/shared"
	"errors"
	"fmt"
	"log"
)

// Executor is the main executor structure
type Executor struct {
	aria            *core.AriaSQL
	ch              *core.Channel
	resultSetBuffer []byte
}

// New creates a new Executor
func New(aria *core.AriaSQL, ch *core.Channel) *Executor {
	return &Executor{ch: ch, aria: aria}
}

// Execute executes a statement
func (ex *Executor) Execute(stmt parser.Statement) error {
	switch stmt := stmt.(type) {
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

				log.Println("well", val)

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
