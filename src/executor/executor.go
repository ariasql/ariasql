package executor

import (
	"ariasql/parser"
	"errors"
)

type Executor struct {
}

func (ex *Executor) Execute(stmt parser.Statement) error {
	switch stmt := stmt.(type) {
	case *parser.SelectStmt:
		err := ex.executeSelectStmt(stmt)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported statement")

	}

	return errors.New("unsupported statement")

}

func (ex *Executor) executeSelectStmt(stmt *parser.SelectStmt) error {

	return nil
}
