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

import (
	"ariasql/catalog"
	"ariasql/shared"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

var (
	keywords = []string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BY", "BIGINT", "BOOLEAN", "BOOL",
		"CHAR", "CHARACTER", "CHECK", "CLOSE", "COBOL", "COMMIT",
		"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "CASCADE",
		"DEC", "DECIMAL", "DECLARE", "DELETE", "DESC", "DISTINCT", "DOUBLE", "DATABASE", "DEFAULT",
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
		"SQL", "SQLCODE", "SQLERROR", "SUM", "DATE", "DATETIME", "TIME", "TIMESTAMP", "BINARY",
		"TABLE", "TO", "UNION", "UNIQUE", "UPDATE", "USER", "FOR", "FOREIGN",
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "UUID", "INDEX", "USE", "TEXT",
		"INNER", "OUTER", "LEFT", "RIGHT", "JOIN", "CROSS", "NATURAL", "FULL", "EXCEPT", "INTERSECT",
	}
)

type TokenType int // Token type

const (
	EOF_TOK        = iota // End of input
	KEYWORD_TOK           // Keywords like SELECT, FROM, WHERE, etc.
	IDENT_TOK             // Identifiers like table names, column names, etc.
	COMMENT_TOK           // Comments
	LITERAL_TOK           // Literals like strings, numbers, etc.
	LPAREN_TOK            // (
	RPAREN_TOK            // )
	SEMICOLON_TOK         // ;
	DATATYPE_TOK          // Data types like INT, CHAR, etc.
	COMMA_TOK             // ,
	ASTERISK_TOK          // *
	COMPARISON_TOK        // =, <>, <, >, <=, >=
	PLUS_TOK              // +
	MINUS_TOK             // -
	DIVIDE_TOK            // /
	MODULUS_TOK           // %
	AT_TOK                // @
)

// Parser is a parser for SQL
type Parser struct {
	lexer *Lexer
	pos   int
}

// Lexer is a lexer for SQL
type Lexer struct {
	input  []byte  // Input to be tokenized
	pos    int     // Position in the input
	tokens []Token // Tokens found
}

// Token is a token found by the lexer
type Token struct {
	tokenT TokenType   // Type of token
	value  interface{} // Value of token
}

// NewLexer creates a new lexer
func NewLexer(input []byte) *Lexer {
	return &Lexer{
		input: input,
	}
}

// isLetter returns true if r is a letter
func isLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || r == '.'
}

// isDigit returns true if r is a digit
func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

// checkKeyword returns true if s is a keyword
func checkKeyword(s string) bool {
	for _, k := range keywords {
		if strings.EqualFold(s, k) {
			return true
		}
	}
	return false
}

// nextToken returns the next token
func (l *Lexer) nextToken() Token {
	insideLiteral := false
	var stringLiteral string // string literal
	quoteChar := byte(0)     // The quote character used to start the string literal

	for {
		if l.pos >= len(l.input) {
			return Token{tokenT: EOF_TOK}
		}

		switch l.input[l.pos] {
		case '-':
			if !insideLiteral {

				if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
					l.pos += 2
					comment := ""
					for l.pos < len(l.input) && l.input[l.pos] != '\n' {
						comment += string(l.input[l.pos])
						l.pos++
					}

					return Token{tokenT: COMMENT_TOK, value: comment}
				}
				l.pos++
				return Token{tokenT: MINUS_TOK, value: "-"}

			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ' ', '\t', '\n': // Skip whitespace
			if !insideLiteral {
				l.pos++
				continue
			}
			stringLiteral += string(l.input[l.pos])
			l.pos++
			continue
		case '\\': // Escape character
			if insideLiteral {
				// If the next character is the same as the quote character, add both the escape character and the quote character to the string literal
				if l.pos+1 < len(l.input) && l.input[l.pos+1] == quoteChar {
					stringLiteral += string(l.input[l.pos]) + string(l.input[l.pos+1])
					l.pos += 2
					continue
				}
			}
			l.pos++
			continue
		case '"', '\'':
			if insideLiteral {
				if l.input[l.pos] == quoteChar {
					// End of string literal
					insideLiteral = false
					stringLiteral += string(l.input[l.pos])
					l.pos++
					return Token{tokenT: LITERAL_TOK, value: stringLiteral}
				} else {
					// Quote character inside string literal
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			} else {
				// Start of string literal
				insideLiteral = true
				quoteChar = l.input[l.pos]
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '=':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: COMPARISON_TOK, value: "="}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '+':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: PLUS_TOK, value: "+"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '/':
			if !insideLiteral {
				if l.input[l.pos+1] == '*' {
					l.pos += 2
					comment := ""
					for l.input[l.pos] != '*' && l.input[l.pos+1] != '/' {
						comment += string(l.input[l.pos])
						l.pos++
					}
					l.pos += 2
					return Token{tokenT: COMMENT_TOK, value: strings.TrimSpace(comment)}
				}
				l.pos++
				return Token{tokenT: DIVIDE_TOK, value: "/"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '%':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: MODULUS_TOK, value: "%"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '@':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: AT_TOK, value: "@"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '<':
			if !insideLiteral {
				if l.input[l.pos+1] == '>' {
					l.pos += 2
					return Token{tokenT: COMPARISON_TOK, value: "<>"}
				} else if l.input[l.pos+1] == '=' {
					l.pos++
					return Token{tokenT: COMPARISON_TOK, value: "<="}
				}
				l.pos++
				return Token{tokenT: COMPARISON_TOK, value: "<"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '>':
			if !insideLiteral {
				if l.input[l.pos+1] == '=' {
					l.pos++
					return Token{tokenT: COMPARISON_TOK, value: ">="}
				}
				l.pos++
				return Token{tokenT: COMPARISON_TOK, value: ">"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '*':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: ASTERISK_TOK, value: "*"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ',':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: COMMA_TOK, value: ","}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case '(':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: LPAREN_TOK, value: "("}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ')':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: RPAREN_TOK, value: ")"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
		case ';':
			if !insideLiteral {
				l.pos++
				return Token{tokenT: SEMICOLON_TOK, value: ";"}
			} else {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}

		default:
			if isLetter(rune(l.input[l.pos])) {
				if !insideLiteral {
					switch l.input[l.pos] {
					case 'T', 't':
						if !insideLiteral {
							// check for TRUE
							if l.input[l.pos+1] == 'R' || l.input[l.pos+1] == 'r' {
								if l.input[l.pos+2] == 'U' || l.input[l.pos+2] == 'u' {
									if l.input[l.pos+3] == 'E' || l.input[l.pos+3] == 'e' {
										l.pos += 4
										return Token{tokenT: LITERAL_TOK, value: true}
									}
								}
							}
						}
					case 'F', 'f':
						if !insideLiteral {
							// check for FALSE
							if l.input[l.pos+1] == 'A' || l.input[l.pos+1] == 'a' {
								if l.input[l.pos+2] == 'L' || l.input[l.pos+2] == 'l' {
									if l.input[l.pos+3] == 'S' || l.input[l.pos+3] == 's' {
										if l.input[l.pos+4] == 'E' || l.input[l.pos+4] == 'e' {
											l.pos += 5
											return Token{tokenT: LITERAL_TOK, value: false}
										}
									}
								}
							}
						}
					}

					startPos := l.pos

					for isDigit(rune(l.input[l.pos])) || isLetter(rune(l.input[l.pos])) {
						l.pos++
						if l.pos+1 > len(l.input) {
							break
						}
					}

					if checkKeyword(string(l.input[startPos:l.pos])) {

						if shared.IsValidDataType(string(l.input[startPos:l.pos])) {
							return Token{tokenT: DATATYPE_TOK, value: string(l.input[startPos:l.pos])}
						} else {
							return Token{tokenT: KEYWORD_TOK, value: strings.ToUpper(string(l.input[startPos:l.pos]))}
						}
					} else {
						parsedUInt, err := strconv.ParseUint(string(l.input[startPos:l.pos]), 10, 32) // convert string to uint
						if err == nil {
							// is parsable uint
							return Token{tokenT: LITERAL_TOK, value: parsedUInt}

						} else {
							return Token{tokenT: IDENT_TOK, value: string(l.input[startPos:l.pos])}
						}
					}
				} else {
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			} else if isDigit(rune(l.input[l.pos])) {
				if !insideLiteral {
					n := ""

					if l.pos+1 < len(l.input) {

						for isDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.' {

							n += string(l.input[l.pos])
							l.pos++
						}
						parsedUInt, err := strconv.ParseUint(n, 10, 32) // convert string to uint
						if err == nil {
							// is parsable uint
							return Token{tokenT: LITERAL_TOK, value: parsedUInt}

						} else {
							parsedFloat, err := strconv.ParseFloat(n, 64) // convert string to float
							if err == nil {
								// is parsable float
								return Token{tokenT: LITERAL_TOK, value: parsedFloat}
							}
						}
					}

				} else {
					stringLiteral += string(l.input[l.pos])
					l.pos++
					continue
				}
			}

			l.pos++
		}
	}
}

// Tokenize tokenizes the input
func (l *Lexer) tokenize() {
	for {
		tok := l.nextToken()
		if tok.tokenT == EOF_TOK {
			break
		}
		l.tokens = append(l.tokens, tok)
	}

	return
}

// NewParser creates a new parser
func NewParser(lexer *Lexer) *Parser {
	return &Parser{
		lexer: lexer,
	}
}

// consume consumes the next token
func (p *Parser) consume() {
	p.pos++
}

// peek returns the next token
func (p *Parser) peek(i int) Token {
	if p.pos+i >= len(p.lexer.tokens) {
		return Token{tokenT: EOF_TOK}
	}
	return p.lexer.tokens[p.pos+i]
}

// rewind goes back one token
func (p *Parser) rewind(i int) {
	p.pos -= i
}

// peekBack returns the previous token
func (p *Parser) peekBack(i int) Token {
	if p.pos-i < len(p.lexer.tokens) || p.pos-i > len(p.lexer.tokens) {
		return Token{tokenT: EOF_TOK}
	}
	return p.lexer.tokens[p.pos-i]
}

// stripComments removes comments from the token list
func (l *Lexer) stripComments() {
	var newTokens []Token
	for _, tok := range l.tokens {
		if tok.tokenT != COMMENT_TOK {
			newTokens = append(newTokens, tok)
		}
	}
	l.tokens = newTokens

}

func (p *Parser) Parse() (Node, error) {
	p.lexer.tokenize()      // Tokenize the input
	p.lexer.stripComments() // Strip comments

	// Check if statement is empty
	if len(p.lexer.tokens) == 0 {
		return nil, errors.New("empty statement")
	}

	if len(p.lexer.tokens) < 3 {
		return nil, errors.New("invalid statement")
	}

	// Check if statement ends with a semicolon
	if p.lexer.tokens[len(p.lexer.tokens)-1].tokenT != SEMICOLON_TOK {
		return nil, errors.New("expected ';'")
	}

	// Check if statement starts with a keyword
	if p.peek(0).tokenT == KEYWORD_TOK {
		switch p.peek(0).value {
		case "CREATE":
			return p.parseCreateStmt()
		case "USE":
			return p.parseUseStmt()
		case "INSERT":
			return p.parseInsertStmt()
		case "SELECT":
			return p.parseSelectStmt()
		}
	}

	return nil, errors.New("expected keyword")
}

// parseCreateStmt parses a CREATE statement
func (p *Parser) parseCreateStmt() (Node, error) {
	p.consume() // Consume CREATE

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "DATABASE":
		return p.parseCreateDatabaseStmt()
	case "SCHEMA":
		return p.parseCreateSchemaStmt()
	case "INDEX", "UNIQUE":
		if p.peek(1).value == "INDEX" {
			// eat unique
			p.consume()

			ast, err := p.parseCreateIndexStmt()
			if err != nil {
				return nil, err
			}

			ast.(*CreateIndexStmt).Unique = true
			return ast, nil
		}
		return p.parseCreateIndexStmt()
	case "TABLE":
		return p.parseCreateTableStmt()
	}

	return nil, errors.New("expected DATABASE or TABLE")
}

// parseCreateDatabaseStmt parses a CREATE DATABASE statement
func (p *Parser) parseCreateDatabaseStmt() (Node, error) {
	p.consume() // Consume DATABASE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &CreateDatabaseStmt{
		Name: &Identifier{Value: name},
	}, nil
}

// parseUseStmt parses a USE statement
func (p *Parser) parseUseStmt() (Node, error) {
	p.consume() // Consume USE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &UseStmt{
		DatabaseName: &Identifier{Value: name},
	}, nil

}

// parseCreateSchemaStmt parses a CREATE SCHEMA statement
func (p *Parser) parseCreateSchemaStmt() (Node, error) {
	p.consume() // Consume SCHEMA

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &CreateSchemaStmt{
		Name: &Identifier{Value: name},
	}, nil

}

// parseCreateIndexStmt parses a CREATE INDEX statement
func (p *Parser) parseCreateIndexStmt() (Node, error) {
	createIndexStmt := &CreateIndexStmt{}
	// CREATE INDEX index_name ON schema_name.table_name (column_name1, column_name2, ...)
	// creating unique index
	// CREATE UNIQUE INDEX index_name ON schema_name.table_name (column_name1, column_name2, ...)

	// Eat INDEX
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	indexName := p.peek(0).value.(string)
	p.consume() // Consume index name

	if p.peek(0).value != "ON" {
		return nil, errors.New("expected ON")
	}

	p.consume() // Consume ON

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)
	p.consume() // Consume table name

	// check if schema_name.table_name is valid
	if len(strings.Split(tableName, ".")) != 2 {
		return nil, errors.New("expected schema_name.table_name")
	}

	schemaName := strings.Split(tableName, ".")[0]
	tableName = strings.Split(tableName, ".")[1]

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")

	}

	p.consume() // Consume (

	createIndexStmt.SchemaName = &Identifier{Value: schemaName}
	createIndexStmt.TableName = &Identifier{Value: tableName}
	createIndexStmt.IndexName = &Identifier{Value: indexName}
	createIndexStmt.ColumnNames = make([]*Identifier, 0)

	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)
		createIndexStmt.ColumnNames = append(createIndexStmt.ColumnNames, &Identifier{Value: columnName})

		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New("expected ,")
		}

		p.consume() // Consume ,

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New("expected )")
	}

	p.consume() // Consume )

	return createIndexStmt, nil
}

// parseInsertStmt parses an INSERT statement
func (p *Parser) parseInsertStmt() (Node, error) {
	// INSERT INTO schema_name.table_name (column_name1, column_name2, ...) VALUES (value1, value2, ...), (value1, value2, ...), ...

	insertStmt := &InsertStmt{}

	// Eat INSERT
	p.consume()

	if p.peek(0).value != "INTO" {
		return nil, errors.New("expected INTO")
	}

	// Eat INTO
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)

	if len(strings.Split(tableName, ".")) != 2 {
		return nil, errors.New("expected schema_name.table_name")
	}

	schemaName := strings.Split(tableName, ".")[0]
	tableName = strings.Split(tableName, ".")[1]

	insertStmt.SchemaName = &Identifier{Value: schemaName}
	insertStmt.TableName = &Identifier{Value: tableName}
	insertStmt.ColumnNames = make([]*Identifier, 0)
	insertStmt.Values = make([][]*Literal, 0)

	p.consume() // Consume schema_name.table_name

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")
	}

	p.consume() // Consume (
	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)
		insertStmt.ColumnNames = append(insertStmt.ColumnNames, &Identifier{Value: columnName})

		p.consume() // Consume column name

		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New("expected ,")
		}

		p.consume() // Consume ,

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New("expected )")
	}

	p.consume() // Consume )

	// Look for VALUES

	if p.peek(0).value != "VALUES" {
		return nil, errors.New("expected VALUES")
	}

	p.consume() // Consume VALUES

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")
	}

	for {
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		p.consume() // Consume (

		values := make([]*Literal, 0)

		for {
			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != LITERAL_TOK {
				return nil, errors.New("expected literal")
			}

			values = append(values, &Literal{Value: p.peek(0).value})

			p.consume() // Consume literal

			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != COMMA_TOK {
				return nil, errors.New("expected ,")
			}

			p.consume() // Consume ,
		}

		insertStmt.Values = append(insertStmt.Values, values)

		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		p.consume() // Consume )

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return nil, errors.New("expected ,")
		}

		p.consume() // Consume ,
	}

	return insertStmt, nil
}

// parseCreateTableStmt parses a CREATE TABLE statement
func (p *Parser) parseCreateTableStmt() (Node, error) {
	// CREATE TABLE schema_name.table_name (column_name1 data_type constraints, column_name2 data_type constraints, ...)
	createTableStmt := &CreateTableStmt{}

	// Eat TABLE
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)

	if len(strings.Split(tableName, ".")) != 2 {
		return nil, errors.New("expected schema_name.table_name")
	}

	schemaName := strings.Split(tableName, ".")[0]
	tableName = strings.Split(tableName, ".")[1]

	createTableStmt.SchemaName = &Identifier{Value: schemaName}
	createTableStmt.TableName = &Identifier{Value: tableName}

	p.consume() // Consume schema_name.table_name

	createTableStmt.TableSchema = &catalog.TableSchema{
		ColumnDefinitions: make(map[string]*catalog.ColumnDefinition),
	}

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")
	}

	p.consume() // Consume (

	for p.peek(0).tokenT != SEMICOLON_TOK {
		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)

		p.consume() // Consume column name

		if p.peek(0).tokenT != DATATYPE_TOK {

			return nil, errors.New("expected data type")
		}

		dataType := p.peek(0).value.(string)

		createTableStmt.TableSchema.ColumnDefinitions[columnName] = &catalog.ColumnDefinition{
			Datatype: dataType,
		}

		p.consume() // Consume data type

		// check for DATATYPE(LEN) or DATATYPE(PRECISION, SCALE)
		if p.peek(0).tokenT == LPAREN_TOK {
			switch dataType {
			case "CHAR", "CHARACTER":
				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {

					return nil, errors.New("expected literal")
				}

				length := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New("expected )")
				}

				p.consume() // Consume )

				createTableStmt.TableSchema.ColumnDefinitions[columnName].Length = int(length)
			case "DEC", "DECIMAL", "NUMERIC":

				p.consume() // Consume (

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New("expected literal")
				}

				precision := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != COMMA_TOK {
					return nil, errors.New("expected ,")
				}

				p.consume() // Consume ,

				if p.peek(0).tokenT != LITERAL_TOK {
					return nil, errors.New("expected literal")
				}

				scale := p.peek(0).value.(uint64)

				p.consume() // Consume literal

				if p.peek(0).tokenT != RPAREN_TOK {
					return nil, errors.New("expected )")
				}

				p.consume() // Consume )

				createTableStmt.TableSchema.ColumnDefinitions[columnName].Precision = int(precision)
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Scale = int(scale)

			}
		}

		// Check for constraints
		if p.peek(0).tokenT == KEYWORD_TOK {
			for p.peek(0).tokenT == KEYWORD_TOK {
				switch p.peek(0).value {
				case "NOT":
					p.consume() // Consume NOT

					if p.peek(0).value != "NULL" {
						return nil, errors.New("expected NULL")
					}

					p.consume() // Consume NULL

					createTableStmt.TableSchema.ColumnDefinitions[columnName].NotNull = true
					continue
				case "PRIMARY":
					p.consume() // Consume PRIMARY

					if p.peek(0).value != "KEY" {
						return nil, errors.New("expected KEY")
					}

					p.consume() // Consume KEY

					createTableStmt.TableSchema.ColumnDefinitions[columnName].PrimaryKey = true
					continue
				case "UNIQUE":
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true

					p.consume() // Consume UNIQUE
					continue
				case "DEFAULT":
					p.consume() // Consume DEFAULT

					if p.peek(0).tokenT != LITERAL_TOK {
						return nil, errors.New("expected literal")
					}

					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = p.peek(0).value

					p.consume() // Consume literal
					continue
				case "SEQUENCE":
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = true

					p.consume() // Consume SEQUENCE
					continue
				case "FOREIGN":
					p.consume() // Consume FOREIGN

					if p.peek(0).value != "KEY" {
						return nil, errors.New("expected KEY")
					}

					p.consume() // Consume KEY
				case "REFERENCES":
					if p.peek(0).value != "REFERENCES" {
						return nil, errors.New("expected REFERENCES")
					}

					p.consume() // Consume REFERENCES

					if p.peek(0).tokenT != IDENT_TOK {
						return nil, errors.New("expected identifier")
					}

					// check if schema_name.table_name is valid
					if len(strings.Split(p.peek(0).value.(string), ".")) != 2 {
						return nil, errors.New("expected schema_name.table_name")
					}

					rSchemaName := strings.Split(p.peek(0).value.(string), ".")[0]
					rTableName := strings.Split(p.peek(0).value.(string), ".")[1]

					createTableStmt.TableSchema.ColumnDefinitions[columnName].ForeignTable = rTableName
					createTableStmt.TableSchema.ColumnDefinitions[columnName].ForeignSchema = rSchemaName
					createTableStmt.TableSchema.ColumnDefinitions[columnName].IsForeign = true

					p.consume() // Consume schema_name.table_name

					if p.peek(0).tokenT != LPAREN_TOK {
						return nil, errors.New("expected (")
					}

					p.consume() // Consume (

					if p.peek(0).tokenT != IDENT_TOK {
						return nil, errors.New("expected identifier")
					}

					rColumnName := p.peek(0).value.(string)

					createTableStmt.TableSchema.ColumnDefinitions[columnName].ForeignColumn = rColumnName

					p.consume() // Consume column name

					if p.peek(0).tokenT != RPAREN_TOK {
						return nil, errors.New("expected )")
					}

					// Consume )
					p.consume()

				case "ON":
					p.consume() // Consume ON

					cascadeOpt := p.peek(0).value.(string)

					if p.peek(0).value == "DELETE" || p.peek(0).value == "UPDATE" {
						p.consume() // Consume DELETE or UPDATE

						if p.peek(0).value == "CASCADE" {
							if cascadeOpt == "DELETE" {
								createTableStmt.TableSchema.ColumnDefinitions[columnName].OnDelete = catalog.CascadeActionCascade
							} else {
								createTableStmt.TableSchema.ColumnDefinitions[columnName].OnUpdate = catalog.CascadeActionCascade
							}
							p.consume() // Consume CASCADE
						} else if p.peek(0).value == "SET" {
							p.consume() // Consume SET

							if p.peek(0).value == "NULL" {
								if cascadeOpt == "DELETE" {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnDelete = catalog.CascadeActionSetNull
								} else {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnUpdate = catalog.CascadeActionSetNull
								}
								p.consume() // Consume NULL
							} else if p.peek(0).value == "DEFAULT" {
								if cascadeOpt == "DELETE" {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnDelete = catalog.CascadeActionSetDefault
								} else {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnUpdate = catalog.CascadeActionSetDefault
								}
								p.consume() // Consume DEFAULT
							} else {
								return nil, errors.New("expected CASCADE, SET NULL or SET DEFAULT")
							}
						} else if p.peek(0).value == "NO" {
							p.consume() // Consume NO

							if p.peek(0).value == "ACTION" {
								if cascadeOpt == "DELETE" {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnDelete = catalog.CascadeActionNone
								} else {
									createTableStmt.TableSchema.ColumnDefinitions[columnName].OnUpdate = catalog.CascadeActionNone
								}
								p.consume() // Consume ACTION
							} else {
								return nil, errors.New("expected ACTION")
							}
						} else if p.peek(0).value == "RESTRICT" {
							if cascadeOpt == "DELETE" {
								createTableStmt.TableSchema.ColumnDefinitions[columnName].OnDelete = catalog.CascadeActionRestrict
							} else {
								createTableStmt.TableSchema.ColumnDefinitions[columnName].OnUpdate = catalog.CascadeActionRestrict
							}
							p.consume() // Consume RESTRICT
						} else {
							return nil, errors.New("expected CASCADE, SET NULL, SET DEFAULT, NO ACTION or RESTRICT")
						}

					}

				default:
					return nil, errors.New("expected PRIMARY, NOT NULL, UNIQUE, DEFAULT, SEQUENCE or FOREIGN KEY")
				}

			}

		}

		p.consume() // Consume ,

	}

	return createTableStmt, nil
}

// parseSelectStmt parses a SELECT statement
func (p *Parser) parseSelectStmt() (Node, error) {
	selectStmt := &SelectStmt{}

	// Eat SELECT
	p.consume()

	// Check for DISTINCT
	if p.peek(0).value == "DISTINCT" {
		selectStmt.Distinct = true
		p.consume()
	}

	// Parse column set
	err := p.parseColumnSet(selectStmt)
	if err != nil {
		return nil, err
	}

	// Check for FROM
	if p.peek(0).value == "FROM" {
		err = p.parseFrom(selectStmt)
		if err != nil {
			return nil, err
		}
	}

	// Check for joins
	if p.peek(0).value == "JOIN" || p.peek(0).value == "INNER" || p.peek(0).value == "LEFT" || p.peek(0).value == "RIGHT" || p.peek(0).value == "FULL" || p.peek(0).value == "CROSS" || p.peek(0).value == "NATURAL" {
		err = p.parseJoin(selectStmt)
		if err != nil {
			return nil, err
		}

	}

	// Check for WHERE
	if p.peek(0).value == "WHERE" {
		err = p.parseWhere(selectStmt)
		if err != nil {
			return nil, err
		}

	}

	// Check for GROUP BY
	if p.peek(0).value == "GROUP" {
		err = p.parseGroupBy(selectStmt)
		if err != nil {
			return nil, err
		}

	}

	// Check for ORDER BY
	if p.peek(0).value == "ORDER" {
		err = p.parseOrderBy(selectStmt)
		if err != nil {
			return nil, err
		}

	}

	// Check for LIMIT-OFFSET
	if p.peek(0).value == "LIMIT" {
		err = p.parseLimit(selectStmt)
		if err != nil {
			return nil, err
		}
	}

	// Check for Union, Intersect, Except
	if p.peek(0).value == "UNION" || p.peek(0).value == "INTERSECT" || p.peek(0).value == "EXCEPT" {
		err = p.parseSetOperation(selectStmt)
		if err != nil {
			return nil, err
		}

	}

	return selectStmt, nil
}

func (p *Parser) parseSetOperation(selectStmt *SelectStmt) error {
	if p.peek(0).value == "UNION" {
		union := &UnionStmt{}
		p.consume()
		if p.peek(0).value == "ALL" {
			union.All = true
			p.consume()

			if p.peek(0).value == "SELECT" {
				sel, err := p.parseSelectStmt()
				if err != nil {
					return err
				}

				union.SelectStmt = sel.(*SelectStmt)
			}
		}
	} else if p.peek(0).value == "INTERSECT" {
		intersect := &IntersectStmt{}
		p.consume()

		if p.peek(0).value == "SELECT" {
			sel, err := p.parseSelectStmt()
			if err != nil {
				return err
			}

			intersect.SelectStmt = sel.(*SelectStmt)
		}

	} else if p.peek(0).value == "EXCEPT" {
		except := &ExceptStmt{}
		p.consume()

		if p.peek(0).value == "SELECT" {
			sel, err := p.parseSelectStmt()
			if err != nil {
				return err
			}

			except.SelectStmt = sel.(*SelectStmt)

		}
	}

	return nil
}

func (p *Parser) parseLimit(selectStmt *SelectStmt) error {
	p.consume() // Consume LIMIT

	if p.peek(0).tokenT != LITERAL_TOK {
		return errors.New("expected literal")
	}

	limit := p.peek(0).value.(uint64)

	selectStmt.Limit = &LimitClause{
		Offset: 0,
		Count:  int(limit),
	}

	p.consume() // Consume literal

	if p.peek(0).value == "OFFSET" {
		p.consume() // Consume OFFSET

		if p.peek(0).tokenT != LITERAL_TOK {
			return errors.New("expected literal")
		}

		offset := p.peek(0).value.(uint64)

		selectStmt.Limit.Offset = int(offset)
	}

	return nil
}

func (p *Parser) parseOrderBy(selectStmt *SelectStmt) error {
	p.consume() // Consume ORDER

	if p.peek(0).value != "BY" {
		return errors.New("expected BY")
	}

	p.consume() // Consume BY

	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)

		if len(strings.Split(columnName, ".")) != 2 {
			return errors.New("expected table_name.column_name, or alias.column_name")
		}

		tableName := strings.Split(columnName, ".")[0]
		columnName = strings.Split(columnName, ".")[1]

		orderBy := []interface{}{}

		orderBy = append(orderBy, ColumnSpec{
			TableName:  &Identifier{Value: tableName},
			ColumnName: &Identifier{Value: tableName},
		})

		selectStmt.OrderBy.Columns = orderBy

		p.consume() // Consume column name

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return errors.New("expected ,")
		}

		p.consume() // Consume ,

	}

	return nil
}

func (p *Parser) parseGroupBy(selectStmt *SelectStmt) error {

	p.consume() // Consume GROUP

	if p.peek(0).value != "BY" {
		return errors.New("expected BY")
	}

	p.consume() // Consume BY

	for {
		if p.peek(0).tokenT != IDENT_TOK {
			return errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)

		if len(strings.Split(columnName, ".")) != 2 {
			return errors.New("expected table_name.column_name, or alias.column_name")
		}

		tableName := strings.Split(columnName, ".")[0]
		columnName = strings.Split(columnName, ".")[1]

		groupBy := []interface{}{}

		groupBy = append(groupBy, ColumnSpec{
			TableName:  &Identifier{Value: tableName},
			ColumnName: &Identifier{Value: tableName},
		})

		selectStmt.GroupBy.Columns = groupBy

		p.consume() // Consume column name

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		if p.peek(0).tokenT != COMMA_TOK {
			return errors.New("expected ,")
		}

		p.consume() // Consume ,

	}

	return nil
}

func (p *Parser) parseJoin(selectStmt *SelectStmt) error {

	// JOIN schema_name.table_name ON column_name1 = column_name2

	join := &Join{}

	var joinType JoinType

	if p.peek(0).value == "JOIN" {
		joinType = InnerJoin
		p.consume() // Consume JOIN
	} else if p.peek(0).value == "INNER" {
		joinType = InnerJoin
		p.consume() // Consume INNER
		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN
	} else if p.peek(0).value == "LEFT" {
		joinType = LeftJoin
		p.consume() // Consume LEFT

		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN
	} else if p.peek(0).value == "RIGHT" {
		joinType = RightJoin
		p.consume() // Consume RIGHT
		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN
	} else if p.peek(0).value == "FULL" {
		joinType = FullJoin
		p.consume() // Consume FULL
		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN
	} else if p.peek(0).value == "CROSS" {
		joinType = CrossJoin
		p.consume() // Consume CROSS

		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN
	} else if p.peek(0).value == "NATURAL" {
		joinType = NaturalJoin
		p.consume() // Consume NATURAL

		// look for outer
		if p.peek(0).value == "OUTER" {
			p.consume() // Consume OUTER
		}

		p.consume() // Consume JOIN

	}

	if p.peek(0).tokenT != IDENT_TOK {
		return errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)

	if len(strings.Split(tableName, ".")) != 2 {
		return errors.New("expected schema_name.table_name")
	}

	schemaName := strings.Split(tableName, ".")[0]
	tableName = strings.Split(tableName, ".")[1]

	rightTable := &Table{
		SchemaName: &Identifier{Value: schemaName},
		TableName:  &Identifier{Value: tableName},
	}

	join.RightTable = rightTable

	join.LeftTable = selectStmt.From.Tables[0]

	join.JoinType = joinType

	p.consume() // Consume schema_name.table_name

	// Look for alias
	if p.peek(0).value == "AS" {
		p.consume() // Consume AS

		if p.peek(0).tokenT != IDENT_TOK {
			return errors.New("expected identifier")
		}

		alias := p.peek(0).value.(string)
		rightTable.Alias = &Identifier{Value: alias}

		p.consume() // Consume alias

	}

	if p.peek(0).value != "ON" {
		return errors.New("expected ON")
	}

	p.consume() // Consume ON

	// Parse join comparison
	err := p.parseComparisonPredicate(join, nil)
	if err != nil {
		return err
	}

	selectStmt.Joins = append(selectStmt.Joins, join)

	return nil
}

func (p *Parser) parseComparisonPredicate(where interface{}, columnSpec *ColumnSpec) error {
	_, ok := where.(*Join)
	if ok {
		colL := p.peek(0).value.(string)

		if len(strings.Split(colL, ".")) != 2 {
			return errors.New("expected table_name.column_name")
		}

		tableNameL := strings.Split(colL, ".")[0]
		columnNameL := strings.Split(colL, ".")[1]

		columnSpecL := &ColumnSpec{
			TableName:  &Identifier{Value: tableNameL},
			ColumnName: &Identifier{Value: columnNameL},
		}

		p.consume() // Consume schema_name.table_name.column_name

		switch p.peek(0).value {
		case "=":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Eq,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[0]
			columnNameR := strings.Split(colR, ".")[1]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		case "<>", "!=":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Ne,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[1]
			columnNameR := strings.Split(colR, ".")[2]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		case "<":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Lt,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[1]
			columnNameR := strings.Split(colR, ".")[2]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		case "<=":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Le,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[1]
			columnNameR := strings.Split(colR, ".")[2]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		case ">":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Gt,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[1]
			columnNameR := strings.Split(colR, ".")[2]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		case ">=":
			compPred := &ComparisonPredicate{
				LeftExpr:  columnSpecL,
				RightExpr: nil,
				Operator:  Ge,
			}

			p.consume() // Consume =

			colR := p.peek(0).value.(string)

			if len(strings.Split(colR, ".")) != 2 {
				return errors.New("expected table_name.column_name")
			}

			tableNameR := strings.Split(colR, ".")[1]
			columnNameR := strings.Split(colR, ".")[2]

			columnSpecR := &ColumnSpec{
				TableName:  &Identifier{Value: tableNameR},
				ColumnName: &Identifier{Value: columnNameR},
			}

			compPred.RightExpr = columnSpecR

			where.(*Join).Cond = compPred

			return nil
		default:
			return errors.New("expected comparison operator")

		}

	}

	switch p.peek(0).value {
	case "=":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Eq,
		}

		p.consume() // Consume =

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	case "<>", "!=":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Ne,
		}

		p.consume() // Consume <>, !=

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	case "<":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Lt,
		}

		p.consume() // Consume <

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	case "<=":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Le,
		}

		p.consume() // Consume <=

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	case ">":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Gt,
		}

		p.consume() // Consume >

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	case ">=":
		compPred := &ComparisonPredicate{
			LeftExpr:  columnSpec,
			RightExpr: nil,
			Operator:  Ge,
		}

		p.consume() // Consume =

		if p.peek(0).tokenT == LITERAL_TOK {
			compPred.RightExpr = &ValueExpr{
				Value: p.peek(0).value,
			}
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = compPred
		case *NotPredicate:
			where.(*NotPredicate).Expr = compPred
		}
	default:
		return errors.New("expected comparison operator")
	}

	p.consume() // Consume value

	return nil
}

func (p *Parser) parseInPredicate(where interface{}, columnSpec *ColumnSpec) error {
	p.consume() // Consume IN

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New("expected (")
	}

	p.consume() // Consume (

	in := &InPredicate{
		Expr:   columnSpec,
		Values: make([]interface{}, 0),
	}

	// Check for subquery
	if p.peek(0).value == "SELECT" {

		subquery, err := p.parseSelectStmt()
		if err != nil {
			return err
		}

		in.Subquery = subquery.(*SelectStmt)

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = in
		case *NotPredicate:
			where.(*NotPredicate).Expr = in
		}

	} else {

		for p.peek(0).tokenT != RPAREN_TOK {
			if p.peek(0).tokenT != LITERAL_TOK {
				return errors.New("expected literal")
			}

			in.Values = append(in.Values, &ValueExpr{
				Value: &Literal{Value: p.peek(0).value},
			})

			p.consume() // Consume literal

			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != COMMA_TOK {
				return errors.New("expected ,")
			}

			p.consume() // Consume ,

		}

		if p.peek(0).tokenT != RPAREN_TOK {
			return errors.New("expected )")
		}

		p.consume() // Consume )

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = in
		case *NotPredicate:
			where.(*NotPredicate).Expr = in
		}
	}

	return nil
}

func (p *Parser) parseBetweenPredicate(where interface{}, columnSpec *ColumnSpec) error {
	p.consume() // Consume BETWEEN

	if p.peek(0).tokenT != LITERAL_TOK {
		return errors.New("expected literal")
	}

	lower := p.peek(0).value

	p.consume() // Consume literal

	if p.peek(0).value != "AND" {
		return errors.New("expected AND")
	}

	p.consume() // Consume AND

	if p.peek(0).tokenT != LITERAL_TOK {
		return errors.New("expected literal")
	}

	upper := p.peek(0).value

	p.consume() // Consume literal

	between := &BetweenPredicate{
		Expr: columnSpec,
		Lower: &ValueExpr{
			Value: &Literal{Value: lower},
		},
		Upper: &ValueExpr{
			Value: &Literal{Value: upper},
		},
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = between
	case *NotPredicate:
		where.(*NotPredicate).Expr = between
	}

	return nil
}

func (p *Parser) parseLikePredicate(where interface{}, columnSpec *ColumnSpec) error {
	p.consume() // consume LIKE
	if p.peek(0).tokenT != LITERAL_TOK {
		return errors.New("expected literal")
	}

	likeExpr := &LikePredicate{
		Expr: columnSpec,
		Pattern: &Literal{
			Value: p.peek(0).value,
		},
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = likeExpr
	case *NotPredicate:
		where.(*NotPredicate).Expr = likeExpr
	}

	p.consume() // consume literal

	return nil
}

func (p *Parser) parseIsPredicate(where interface{}, columnSpec *ColumnSpec) error {
	p.consume() // consume IS

	// IS NULL or IS NOT NULL
	if p.peek(0).value == "NULL" {
		isExpr := &IsNullPredicate{
			Expr: columnSpec,
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = isExpr
		case *NotPredicate:
			where.(*NotPredicate).Expr = isExpr
		}

		p.consume() // consume NULL

	} else if p.peek(0).value == "NOT" {
		p.consume() // consume NOT

		if p.peek(0).value != "NULL" {
			return errors.New("expected NULL")
		}

		isExpr := &IsNotNullPredicate{
			Expr: columnSpec,
		}

		switch where.(type) {
		case *WhereClause:
			where.(*WhereClause).Cond = isExpr
		case *NotPredicate:
			where.(*NotPredicate).Expr = isExpr
		}

		p.consume() // consume NULL
	}

	return nil
}

func (p *Parser) parseExistsPredicate(where interface{}, columnSpec *ColumnSpec) error {
	// SELECT * FROM table_name WHERE EXISTS (SELECT * FROM table_name WHERE condition)
	p.consume() // consume EXISTS

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New("expected (")
	}

	p.consume() // consume (

	if p.peek(0).value != "SELECT" {
		return errors.New("expected SELECT")
	}

	subquery, err := p.parseSelectStmt()

	if err != nil {
		return err
	}

	existsExpr := &ExistsPredicate{
		SelectStmt: subquery.(*SelectStmt),
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = existsExpr
	case *NotPredicate:
		where.(*NotPredicate).Expr = existsExpr
	}

	return nil
}

func (p *Parser) parseAnyPredicate(where interface{}, columnSpec *ColumnSpec) error {
	// SELECT * FROM table_name WHERE column_name operator ANY (SELECT * FROM table_name WHERE condition)
	p.consume() // consume ANY

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New("expected (")
	}

	p.consume() // consume (

	if p.peek(0).value != "SELECT" {
		return errors.New("expected SELECT")
	}

	subquery, err := p.parseSelectStmt()

	if err != nil {
		return err
	}

	anyExpr := &AnyPredicate{
		SelectStmt: subquery.(*SelectStmt),
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = anyExpr
	case *NotPredicate:
		where.(*NotPredicate).Expr = anyExpr
	}

	return nil
}

func (p *Parser) parseAllPredicate(where interface{}, columnSpec *ColumnSpec) error {
	// SELECT * FROM table_name WHERE column_name operator ALL (SELECT * FROM table_name WHERE condition)
	p.consume() // consume ALL

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New("expected (")
	}

	p.consume() // consume (

	if p.peek(0).value != "SELECT" {
		return errors.New("expected SELECT")
	}

	subquery, err := p.parseSelectStmt()

	if err != nil {
		return err
	}

	allExpr := &AllPredicate{
		SelectStmt: subquery.(*SelectStmt),
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = allExpr
	case *NotPredicate:
		where.(*NotPredicate).Expr = allExpr
	}

	return nil
}

func (p *Parser) parseSomePredicate(where interface{}, columnSpec *ColumnSpec) error {
	// SELECT * FROM table_name WHERE column_name operator SOME (SELECT * FROM table_name WHERE condition)
	p.consume() // consume ALL

	if p.peek(0).tokenT != LPAREN_TOK {
		return errors.New("expected (")
	}

	p.consume() // consume (

	if p.peek(0).value != "SELECT" {
		return errors.New("expected SELECT")
	}

	subquery, err := p.parseSelectStmt()

	if err != nil {
		return err
	}

	someExpr := &SomePredicate{
		SelectStmt: subquery.(*SelectStmt),
	}

	switch where.(type) {
	case *WhereClause:
		where.(*WhereClause).Cond = someExpr
	case *NotPredicate:
		where.(*NotPredicate).Expr = someExpr
	}

	return nil
}

func (p *Parser) parseNotPredicate(where *WhereClause, columnSpec *ColumnSpec) error {
	not := &NotPredicate{}

	p.consume() // consume NOT

	switch p.peek(0).value {
	case "IN":
		err := p.parseInPredicate(not, columnSpec)
		if err != nil {
			return err
		}
	case "BETWEEN":
		err := p.parseBetweenPredicate(not, columnSpec)
		if err != nil {
			return err
		}

	case "LIKE":
		err := p.parseLikePredicate(not, columnSpec)
		if err != nil {
			return err
		}

	case "IS":
		err := p.parseIsPredicate(not, columnSpec)
		if err != nil {
			return err
		}

	case "EXISTS":
		err := p.parseExistsPredicate(not, columnSpec)
		if err != nil {
			return err
		}

	case "ANY":
		err := p.parseAnyPredicate(not, columnSpec)
		if err != nil {
			return err
		}
	case "ALL":
		err := p.parseAllPredicate(not, columnSpec)
		if err != nil {
			return err
		}

	case "SOME":
		err := p.parseSomePredicate(not, columnSpec)
		if err != nil {
			return err
		}

	default:
		err := p.parseComparisonPredicate(not, columnSpec)
		if err != nil {
			return err

		}
	}

	where.Cond = not

	return nil
}

// parseWhere parses the WHERE clause of a SELECT statement
func (p *Parser) parseWhere(selectStmt *SelectStmt) error {
	p.consume() // Consume WHERE

	where := &WhereClause{
		Cond: nil,
	}

	// Parse condition
	switch p.peek(0).tokenT {
	case IDENT_TOK:
		// Check if we need to parse binary expression or column spec
		if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK {
			// Parse binary expression
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return err
			}

			where.Cond = expr
		} else {
			// Parse column spec
			columnSpec, err := p.parseColumnSpec()
			if err != nil {
				return err
			}

			// Check for predicate
			if p.peek(0).tokenT == COMPARISON_TOK {
				err = p.parseComparisonPredicate(where, columnSpec)
				if err != nil {
					return err
				}

			} else if p.peek(0).tokenT == KEYWORD_TOK {
				switch p.peek(0).value {
				case "IN":
					err = p.parseInPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "BETWEEN":
					err = p.parseBetweenPredicate(where, columnSpec)
					if err != nil {
						return err
					}

				case "LIKE":
					err = p.parseLikePredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "IS":
					err = p.parseIsPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "EXISTS":
					err = p.parseExistsPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "ANY":
					err = p.parseAnyPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "ALL":
					err = p.parseAllPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "SOME":
					err = p.parseSomePredicate(where, columnSpec)
					if err != nil {
						return err
					}
				case "NOT":
					err = p.parseNotPredicate(where, columnSpec)
					if err != nil {
						return err
					}
				}
			}
		}

	case LITERAL_TOK:
		return errors.New("expected identifier")
	case LPAREN_TOK:
		// Parse binary expression or subquery
		if p.peek(1).value == "SELECT" {
			subquery, err := p.parseSelectStmt()
			if err != nil {
				return err
			}

			where.Cond = subquery
		} else {
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return err
			}

			where.Cond = expr

		}
	}

	selectStmt.Where = where

	return nil

}

// parseFrom parses the FROM clause of a SELECT statement
func (p *Parser) parseFrom(selectStmt *SelectStmt) error {
	p.consume() // Consume FROM

	from := &FromClause{
		Tables: make([]*Table, 0),
	}

	for p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).value != "WHERE" || p.peek(0).value != "JOIN" || p.peek(0).value != "INNER" || p.peek(0).value != "LEFT" || p.peek(0).value != "RIGHT" || p.peek(0).value != "FULL" || p.peek(0).value != "CROSS" || p.peek(0).value != "NATURAL" {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		}

		if p.peek(0).tokenT == KEYWORD_TOK {
			if p.peek(0).value == "WHERE" || p.peek(0).value == "JOIN" || p.peek(0).value == "INNER" || p.peek(0).value == "LEFT" || p.peek(0).value == "RIGHT" || p.peek(0).value == "FULL" || p.peek(0).value == "CROSS" || p.peek(0).value == "NATURAL" {
				break
			}
		}

		if p.peek(0).tokenT != IDENT_TOK {
			if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).tokenT == RPAREN_TOK {
				break
			}
			return errors.New("expected identifier")
		}

		tableName := p.peek(0).value.(string)

		if len(strings.Split(tableName, ".")) != 2 {
			return errors.New("expected schema_name.table_name")
		}

		schemaName := strings.Split(tableName, ".")[0]
		tableName = strings.Split(tableName, ".")[1]

		table := &Table{
			SchemaName: &Identifier{Value: schemaName},
			TableName:  &Identifier{Value: tableName},
		}

		p.consume() // Consume schema_name.table_name

		if p.peek(0).tokenT == KEYWORD_TOK {
			if p.peek(0).value == "AS" {
				p.consume()

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New("expected identifier")
				}

				alias := p.peek(0).value.(string)
				table.Alias = &Identifier{Value: alias}

				p.consume()
			}
		}

		from.Tables = append(from.Tables, table)

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

	}

	selectStmt.From = from

	return nil

}

// parseColumnSet parses the column set of a SELECT statement
func (p *Parser) parseColumnSet(selectStmt *SelectStmt) error {
	columnSet := &ColumnSet{
		Exprs: make([]interface{}, 0),
	}

	for p.peek(0).value != "FROM" || p.peek(0).tokenT != SEMICOLON_TOK {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		} else if p.peek(0).tokenT == KEYWORD_TOK {
			if p.peek(0).value == "FROM" {
				break
			}
		}

		// can be binary expression, column spec, or aggregate function
		if p.peek(0).tokenT == ASTERISK_TOK {
			// if we encounter an asterisk, we add all columns and no more columns nor expressions can be added
			columnSet.Exprs = append(columnSet.Exprs, &ColumnSpec{
				SchemaName: nil,
				TableName:  nil,
				ColumnName: &Identifier{
					Value: "*",
				},
			})

			p.consume()

			selectStmt.ColumnSet = columnSet

			return nil

		}

		if p.peek(0).tokenT == IDENT_TOK {
			// Check if we need to parse binary expression or column spec
			if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK {
				// Parse binary expression
				expr, err := p.parseBinaryExpr(0)
				if err != nil {
					return err
				}

				ve := &ValueExpr{
					Value: expr,
					Alias: nil,
				}

				// Check for alias
				if p.peek(0).tokenT == KEYWORD_TOK {
					if p.peek(0).value == "AS" {
						p.consume()

						if p.peek(0).tokenT != IDENT_TOK {
							return errors.New("expected identifier")
						}

						alias := p.peek(0).value.(string)
						ve.Alias = &Identifier{Value: alias}

						p.consume()
					}
				}

				columnSet.Exprs = append(columnSet.Exprs, ve)
			} else {
				// Parse column spec
				columnSpec, err := p.parseColumnSpec()
				if err != nil {
					return err
				}

				columnSet.Exprs = append(columnSet.Exprs, columnSpec)

			}

			if p.peek(0).tokenT == SEMICOLON_TOK {
				break
			}
		} else if p.peek(0).tokenT == LITERAL_TOK {
			var ve *ValueExpr
			if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK {
				// Parse binary expression
				expr, err := p.parseBinaryExpr(0)
				if err != nil {
					return err
				}

				ve = &ValueExpr{
					Value: expr,
					Alias: nil,
				}
			} else {
				// Parse literal
				literal, err := p.parseLiteral()
				if err != nil {
					return err
				}

				ve = &ValueExpr{
					Value: literal,
					Alias: nil,
				}
			}
			// Check for alias
			if p.peek(0).tokenT == KEYWORD_TOK {
				if p.peek(0).value == "AS" {
					p.consume()

					if p.peek(0).tokenT != IDENT_TOK {
						return errors.New("expected identifier")
					}

					alias := p.peek(0).value.(string)
					ve.Alias = &Identifier{Value: alias}

					p.consume()
				}
			}

			columnSet.Exprs = append(columnSet.Exprs, ve)

			if p.peek(0).tokenT == SEMICOLON_TOK {
				break
			}
		} else {
			return errors.New("expected identifier or literal")

		}

	}

	selectStmt.ColumnSet = columnSet

	return nil
}

func (p *Parser) parseBinaryExpr(precedence int) (interface{}, error) {
	left, err := p.parsePrimaryExpr()
	if err != nil {
		return nil, err
	}

	for {
		nextPrecedence := p.getPrecedence(p.peek(0).tokenT)

		if nextPrecedence <= precedence {
			return left, nil
		}

		op := p.peek(0).value

		p.consume()

		right, err := p.parseBinaryExpr(nextPrecedence)
		if err != nil {
			return nil, err
		}

		left = &BinaryExpr{Left: left, Op: op.(string), Right: right}
	}
}

func (p *Parser) parsePrimaryExpr() (interface{}, error) {
	if p.peek(0).tokenT == LPAREN_TOK {
		p.consume()

		expr, err := p.parseBinaryExpr(0)
		if err != nil {
			return nil, err
		}

		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		p.consume()

		return expr, nil
	}

	return p.parseUnaryExpr()
}

func (p *Parser) parseUnaryExpr() (interface{}, error) {
	if p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == DIVIDE_TOK {
		op := p.peek(0).value.(string)

		p.consume()

		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}

		return &UnaryExpr{Op: op, Expr: expr}, nil
	}

	switch p.peek(0).tokenT {
	case LITERAL_TOK:
		return p.parseLiteral()
	case IDENT_TOK:
		return p.parseColumnSpec()
	case KEYWORD_TOK:
		switch p.peek(0).value {
		case "AVG", "COUNT", "MAX", "MIN", "SUM":
			return p.parseAggregateFunc()

		default:
			return nil, errors.New("expected aggregate function")
		}
	default:
		return nil, errors.New("expected literal or column spec")
	}
}

func (p *Parser) parseAggregateFunc() (*AggFunc, error) {
	// Eat aggregate function
	aggFunc := &AggFunc{FuncName: p.peek(0).value.(string)}

	p.consume()

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")
	}

	p.consume() // Consume (

	for p.peek(0).tokenT != RPAREN_TOK && (p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).tokenT != COMMA_TOK || p.peek(0).value != "FROM") {
		// Catch nested aggregate functions, binary expressions, column specs, and literals
		if p.peek(0).tokenT == KEYWORD_TOK {
			switch p.peek(0).value {
			case "AVG", "COUNT", "MAX", "MIN", "SUM":
				// Parse aggregate function
				innerAggFunc, err := p.parseAggregateFunc()
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, innerAggFunc)
			default:
				return nil, errors.New("expected aggregate function")
			}
		} else if p.peek(0).tokenT == LPAREN_TOK {
			// Parse binary expression
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return nil, err
			}

			aggFunc.Args = append(aggFunc.Args, expr)

		} else if p.peek(0).tokenT == IDENT_TOK {
			if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK {
				// Parse binary expression
				expr, err := p.parseBinaryExpr(0)
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, expr)
			} else {
				// Parse column spec
				columnSpec, err := p.parseColumnSpec()
				if err != nil {
					return nil, err
				}

				aggFunc.Args = append(aggFunc.Args, columnSpec)
			}
		} else {
			return nil, errors.New("expected aggregate function, binary expression, or column spec")
		}

	}

	if p.peek(0).tokenT != RPAREN_TOK {
		return nil, errors.New("expected )")
	}

	p.consume()

	return aggFunc, nil
}

func (p *Parser) parseLiteral() (*Literal, error) {
	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New("expected literal")
	}

	literal := &Literal{Value: p.peek(0).value}
	p.consume()

	return literal, nil
}

func (p *Parser) getPrecedence(tokenT TokenType) int {
	switch tokenT {
	case ASTERISK_TOK, DIVIDE_TOK:
		return 2
	case PLUS_TOK, MINUS_TOK:
		return 1
	default:
		return 0
	}
}

// parseColumnSpec parses a column spec
func (p *Parser) parseColumnSpec() (*ColumnSpec, error) {
	columnSpec := &ColumnSpec{}

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	// schema_name.table_name.column_name
	// Check for alias

	if len(strings.Split(p.peek(0).value.(string), ".")) == 3 {
		schemaName := strings.Split(p.peek(0).value.(string), ".")[0]
		tableName := strings.Split(p.peek(0).value.(string), ".")[1]
		columnName := strings.Split(p.peek(0).value.(string), ".")[2]

		columnSpec.SchemaName = &Identifier{Value: schemaName}
		columnSpec.TableName = &Identifier{Value: tableName}
		columnSpec.ColumnName = &Identifier{Value: columnName}
	} else if len(strings.Split(p.peek(0).value.(string), ".")) == 2 {
		// alias.column_name
		tableName := strings.Split(p.peek(0).value.(string), ".")[0]
		columnName := strings.Split(p.peek(0).value.(string), ".")[1]

		columnSpec.TableName = &Identifier{Value: tableName}
		columnSpec.ColumnName = &Identifier{Value: columnName}
	} else {
		return nil, errors.New("expected schema_name.table_name.column_name")
	}

	p.consume() // Consume column name
	// Check for alias
	if p.peek(0).tokenT == KEYWORD_TOK {
		if p.peek(0).value == "AS" {
			p.consume()

			if p.peek(0).tokenT != IDENT_TOK {
				return nil, errors.New("expected identifier")
			}

			alias := p.peek(0).value.(string)
			columnSpec.Alias = &Identifier{Value: alias}

			p.consume()
		}
	}

	return columnSpec, nil
}

func PrintAST(node Node) (string, error) {
	marshalled, err := json.MarshalIndent(node, "", "  ")
	if err != nil {
		return "", err
	}

	return string(marshalled), nil

}
