// Package parser
// AriaSQL parser package
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
package parser

var (
	keywords = []string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BY", "BIGINT",
		"CHAR", "CHARACTER", "CHECK", "CLOSE", "COBOL", "COMMIT",
		"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "CASCADE",
		"DEC", "DECIMAL", "DECLARE", "DELETE", "DESC", "DISTINCT", "DOUBLE",
		"END", "ESCAPE", "EXEC", "EXISTS",
		"FETCH", "FLOAT", "FOR", "FORTRAN", "FOUND", "FROM",
		"GO", "GOTO", "GRANT", "GROUP", "HAVING",
		"IN", "INDICATOR", "INSERT", "INT", "INTEGER", "INTO", "IS",
		"LANGUAGE", "LIKE", "NO", "ACTION",
		"MAX", "MIN", "MODULE", "NOT", "NULL", "NUMERIC",
		"OF", "ON", "OPEN", "OPTION", "OR", "ORDER",
		"PASCAL", "PLI", "PRECISION", "PRIVILEGES", "PROCEDURE", "PUBLIC", "PRIMARY",
		"REAL", "ROLLBACK", "KEY", "REFERENCES", "RESTRICT",
		"SCHEMA", "SECTION", "SELECT", "SET", "SMALLINT", "SOME", "SEQUENCE",
		"SQL", "SQLCODE", "SQLERROR", "SUM",
		"TABLE", "TO", "UNION", "UNIQUE", "UPDATE", "USER", "FOR", "FOREIGN",
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "UUID",
	}
)
