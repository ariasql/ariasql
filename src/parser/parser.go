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
	"ariasql/shared"
	"errors"
	"strconv"
	"strings"
)

var (
	keywords = []string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BY", "BIGINT",
		"CHAR", "CHARACTER", "CHECK", "CLOSE", "COBOL", "COMMIT",
		"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "CASCADE",
		"DEC", "DECIMAL", "DECLARE", "DELETE", "DESC", "DISTINCT", "DOUBLE", "DATABASE",
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
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "UUID", "INDEX",
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

				if l.input[l.pos+1] == '-' {
					l.pos += 2
					comment := ""
					for l.input[l.pos] != '\n' {
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

// parseCreateTableStmt parses a CREATE TABLE statement
func (p *Parser) parseCreateTableStmt() (Node, error) {

	return nil, nil
}

// parseSelectStmt parses a SELECT statement
func (p *Parser) parseSelectStmt() (Node, error) {
	return nil, nil
}
