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

// Parser is following American National Standard SQL 1986

import (
	"ariasql/catalog"
	"ariasql/shared"
	"errors"
	"strconv"
	"strings"
)

var (
	keywords = append([]string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BY", "CHECK", "CLOSE", "COBOL", "COMMIT",
		"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "DECLARE", "DELETE", "DROP", "DESC", "DISTINCT", "DATABASE",
		"END", "ESCAPE", "EXEC", "EXISTS",
		"FETCH", "FOR", "FORTRAN", "FOUND", "FROM",
		"GO", "GOTO", "GRANT", "GROUP", "HAVING",
		"IN", "INDEX", "INDICATOR", "INSERT", "INTO", "IS", "SEQUENCE",
		"LANGUAGE", "LIKE",
		"MAX", "MIN", "MODULE", "NOT", "NULL",
		"OF", "ON", "OPEN", "OPTION", "OR", "ORDER",
		"PASCAL", "PLI", "PRECISION", "PRIVILEGES", "PROCEDURE", "PUBLIC", "ROLLBACK",
		"SCHEMA", "SECTION", "SELECT", "SET", "SOME",
		"SQL", "SQLCODE", "SQLERROR", "SUM",
		"TABLE", "TO", "UNION", "UNIQUE", "UPDATE", "USER",
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "USE",
	}, shared.DataTypes...)
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
					l.pos++ // skip =
					l.pos++ // skip >
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
		case "DROP":
			return p.parseDropStmt()
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

// parseDropStmt parses a DROP statement
func (p *Parser) parseDropStmt() (Node, error) {
	p.consume() // Consume DROP

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "DATABASE":
		return p.parseDropDatabaseStmt()
	case "TABLE":
		return p.parseDropTableStmt()
	case "INDEX":
		return p.parseDropIndexStmt()
	}

	return nil, errors.New("expected DATABASE or TABLE")

}

// parseDropTableStmt parses a DROP TABLE statement
func (p *Parser) parseDropTableStmt() (Node, error) {
	p.consume() // Consume TABLE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &DropTableStmt{
		TableName: &Identifier{Value: tableName},
	}, nil

}

// parseDropIndexStmt parses a DROP INDEX statement
func (p *Parser) parseDropIndexStmt() (Node, error) {
	p.consume() // Consume INDEX

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	indexName := p.peek(0).value.(string)
	p.consume() // Consume identifier

	if p.peek(0).value != "ON" {
		return nil, errors.New("expected ON")
	}

	p.consume() // Consume ON

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)
	p.consume() // Consume table name

	return &DropIndexStmt{
		TableName: &Identifier{Value: tableName},
		IndexName: &Identifier{Value: indexName},
	}, nil

}

// parseDropDatabaseStmt parses a DROP DATABASE statement
func (p *Parser) parseDropDatabaseStmt() (Node, error) {
	p.consume() // Consume DATABASE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	name := p.peek(0).value.(string)
	p.consume() // Consume identifier

	return &DropDatabaseStmt{
		Name: &Identifier{Value: name},
	}, nil
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

// parseCreateStmt parses a CREATE statement
func (p *Parser) parseCreateStmt() (Node, error) {
	p.consume() // Consume CREATE

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "DATABASE":
		return p.parseCreateDatabaseStmt()
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

	return nil, errors.New("expected DATABASE or TABLE or INDEX")

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
			DataType: dataType,
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
			case "DEC", "DECIMAL", "NUMERIC", "REAL", "FLOAT", "DOUBLE":

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
				case "UNIQUE":
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true

					p.consume() // Consume UNIQUE
					continue
				case "SEQUENCE":
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = true

					p.consume() // Consume SEQUENCE
					continue
				default:
					return nil, errors.New("expected NOT NULL or UNIQUE or SEQUENCE")
				}

			}

		}

		p.consume() // Consume ,

	}

	return createTableStmt, nil
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

	if p.peek(0).tokenT != LPAREN_TOK {
		return nil, errors.New("expected (")

	}

	p.consume() // Consume (

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

	// Parse select list
	err := p.parseSelectList(selectStmt)
	if err != nil {
		return nil, err
	}

	// Check for FROM
	if p.peek(0).value == "FROM" {
		tableExpr, err := p.parseTableExpression()
		if err != nil {
			return nil, err

		}

		selectStmt.TableExpression = tableExpr

	}

	// Check for WHERE
	if p.peek(0).value == "WHERE" {
		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.WhereClause = whereClause

	}

	return selectStmt, nil

}

// parseWhereClause parses a WHERE clause
func (p *Parser) parseWhereClause() (*WhereClause, error) {
	whereClause := &WhereClause{}

	// Eat WHERE
	p.consume()

	// Parse search condition
	searchCondition, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	whereClause.SearchCondition = searchCondition

	return whereClause, nil

}

// parseSearchCondition parses a search condition
func (p *Parser) parseSearchCondition() (interface{}, error) {
	// A search condition can be a binary expression, comparison expression, or a logical expression

	if p.peek(1).tokenT == COMPARISON_TOK || p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK || p.peek(1).tokenT == AT_TOK {
		// Parse comparison expression
		expr, err := p.parseComparisonExpr()
		if err != nil {
			return nil, err
		}

		return expr, nil
	}

	if p.peek(1).tokenT == KEYWORD_TOK {
		if p.peek(1).value == "AND" || p.peek(1).value == "OR" {
			// Parse logical expression
			expr, err := p.parseLogicalExpr()
			if err != nil {
				return nil, err
			}

			return expr, nil
		}
	}

	// Parse binary expression
	expr, err := p.parseBinaryExpr(0)
	if err != nil {
		return nil, err
	}

	return expr, nil

}

// parseComparisonExpr parses a comparison expression
func (p *Parser) parseComparisonExpr() (*ComparisonPredicate, error) {
	// Parse left side of comparison
	left, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	// Parse comparison operator
	op := p.peek(0).value.(string)

	p.consume()

	// Parse right side of comparison
	right, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &ComparisonPredicate{
		Left:  left,
		Op:    getComparisonOperator(op),
		Right: right,
	}, nil
}

// parseLogicalExpr parses a logical expression
func (p *Parser) parseLogicalExpr() (*LogicalCondition, error) {
	// Parse left side of logical expression
	left, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	// Parse logical operator
	op := p.peek(1).value.(string)
	p.consume()

	// Parse right side of logical expression
	right, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	return &LogicalCondition{
		Left:  left,
		Op:    getLogicalOperator(op),
		Right: right,
	}, nil
}

// parseTableExpression parses a table expression
func (p *Parser) parseTableExpression() (*TableExpression, error) {
	tableExpr := &TableExpression{}

	// Eat FROM
	p.consume()

	// Parse from clause
	fromClause, err := p.parseFromClause()
	if err != nil {
		return nil, err
	}

	tableExpr.FromClause = fromClause

	return tableExpr, nil
}

// parseFromClause parses a FROM clause
func (p *Parser) parseFromClause() (*FromClause, error) {
	fromClause := &FromClause{
		Tables: make([]*Table, 0),
	}

	for p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).value != "WHERE" {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		}

		if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).value == "WHERE" {
			break
		}

		// Parse table
		table, err := p.parseTable()
		if err != nil {
			return nil, err
		}

		fromClause.Tables = append(fromClause.Tables, table)
	}

	return fromClause, nil
}

// parseTable parses a table
func (p *Parser) parseTable() (*Table, error) {
	table := &Table{}

	// Parse table name
	tableName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	table.Name = tableName

	return table, nil
}

// parseSelectList parses a select list
func (p *Parser) parseSelectList(selectStmt *SelectStmt) error {
	selectList := &SelectList{
		Expressions: make([]*ValueExpression, 0),
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
			selectList.Expressions = append(selectList.Expressions, &ValueExpression{
				Value: &Wildcard{},
			})

			p.consume()

			selectStmt.SelectList = selectList

			return nil

		}

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		}

		// Parse value expression
		valueExpr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		selectList.Expressions = append(selectList.Expressions, valueExpr)
	}

	selectStmt.SelectList = selectList

	return nil

}

// parseValueExpression parses a value expression
func (p *Parser) parseValueExpression() (*ValueExpression, error) {
	// A value expression can be a binary expression, column spec, or aggregate function

	if p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK {
		// Parse binary expression
		expr, err := p.parseBinaryExpr(0)
		if err != nil {
			return nil, err
		}

		return &ValueExpression{
			Value: expr,
		}, nil
	}

	switch p.peek(0).tokenT {
	case LITERAL_TOK:
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		return &ValueExpression{
			Value: lit,
		}, nil

	case KEYWORD_TOK:
		switch p.peek(0).value {
		case "COUNT", "MAX", "MIN", "SUM", "AVG":
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return nil, err
			}

			return &ValueExpression{
				Value: expr,
			}, nil
		default:
			return nil, errors.New("expected aggregate function")
		}
	case IDENT_TOK:

		// Parse column spec
		colSpec, err := p.parseColumnSpecification()
		if err != nil {
			return nil, err
		}

		return &ValueExpression{
			Value: colSpec,
		}, nil
	default:

		return nil, errors.New("expected column spec or aggregate function")
	}

}

// parseColumnSpecification parses a column specification
func (p *Parser) parseColumnSpecification() (*ColumnSpecification, error) {

	// A column specification is in the form of table_name.column_name or column_name depending on FROM

	// Parse column name
	columnName, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}

	if len(strings.Split(columnName.Value, ".")) == 2 {
		tableName := &Identifier{
			Value: strings.Split(columnName.Value, ".")[0],
		}
		columnName = &Identifier{
			Value: strings.Split(columnName.Value, ".")[1],
		}
		return &ColumnSpecification{
			TableName:  tableName,
			ColumnName: columnName,
		}, nil
	}

	return &ColumnSpecification{
		ColumnName: columnName,
	}, nil
}

// parseIdentifier parses an identifier
func (p *Parser) parseIdentifier() (*Identifier, error) {
	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	ident := &Identifier{
		Value: p.peek(0).value.(string),
	}

	p.consume()

	return ident, nil

}

// parseAggregateFunc parses an aggregate function
func (p *Parser) parseAggregateFunc() (*AggregateFunc, error) {
	// Eat aggregate function
	aggFunc := &AggregateFunc{FuncName: p.peek(0).value.(string)}

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
				columnSpec, err := p.parseColumnSpecification()
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

// getPrecendence returns the precedence of an arithmetic operator
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

// parseBinaryExpr parses a binary expression
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

		left = &BinaryExpression{Left: left, Op: getBinaryExpressionOperator(op.(string)), Right: right}
	}
}

// parsePrimaryExpr parses a primary expression
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

// parseUnaryExpr parses a unary expression
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
		return p.parseColumnSpecification()
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

// parseLiteral parses a literal
func (p *Parser) parseLiteral() (interface{}, error) {
	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New("expected literal")
	}

	lit := p.peek(0).value

	p.consume()

	return &Literal{Value: lit}, nil
}
