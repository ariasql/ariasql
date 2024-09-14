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

// Parser is following American National Standard SQL 1986/SQL-87/SQL-89/SQL-92+

import (
	"ariasql/catalog"
	"ariasql/shared"
	"errors"
	"log"
	"strconv"
	"strings"
)

var (
	keywords = append([]string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG", "ALTER",
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
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK", "USE", "LIMIT", "OFFSET", "IDENTIFIED", "CONNECT", "REVOKE", "SHOW",
		"PRIMARY", "FOREIGN", "KEY", "REFERENCES", "DATE", "TIME", "TIMESTAMP", "DATETIME", "UUID", "BINARY", "DEFAULT",
		"UPPER", "LOWER", "CAST", "COALESCE", "REVERSE", "ROUND", "POSITION", "LENGTH", "REPLACE",
		"CONCAT", "SUBSTRING", "TRIM", "GENERATE_UUID", "SYS_DATE", "SYS_TIME", "SYS_TIMESTAMP", "SYS_DATETIME",
		"CASE", "WHEN", "THEN", "ELSE", "END", "IF", "ELSEIF", "DEALLOCATE", "NEXT", "WHILE", "PRINT",
		"OVER", "PARTITION", "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW",
		"DENSE_RANK", "NTILE", "LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE", "PERCENT_RANK", "CUME_DIST",
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
	//rewrites(&input)

	return &Lexer{
		input: input,
	}
}

// isLetter returns true if r is a letter
func isLetter(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || r == '.' || r == '*'
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
		case '!':
			if insideLiteral {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
			continue
		case '$':
			if insideLiteral {
				stringLiteral += string(l.input[l.pos])
				l.pos++
				continue
			}
			continue
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

// switch switches one token with another
func (p *Parser) switchToken(i, j int) {
	p.lexer.tokens[p.pos+i], p.lexer.tokens[p.pos+j] = p.lexer.tokens[p.pos+j], p.lexer.tokens[p.pos+i]

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

// Parse parses the input
func (p *Parser) Parse() (Node, error) {
	p.lexer.tokenize()      // Tokenize the input
	p.lexer.stripComments() // Strip comments

	// Check if statement is empty
	if len(p.lexer.tokens) == 0 {
		return nil, errors.New("empty statement")
	}

	if len(p.lexer.tokens) < 1 {
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
		case "UPDATE":
			return p.parseUpdateStmt()
		case "DELETE":
			return p.parseDeleteStmt()
		case "BEGIN":
			return p.parseBeginStmt()
		case "COMMIT":
			return p.parseCommitStmt()
		case "ROLLBACK":
			return p.parseRollbackStmt()
		case "GRANT":
			return p.parseGrantStmt()
		case "REVOKE":
			return p.parseRevokeStmt()
		case "SHOW":
			return p.parseShowStmt()
		case "ALTER":
			return p.parseAlterStmt()
		case "DECLARE":
			return p.parseDeclareStmt()
		case "OPEN":
			return p.parseOpenStmt()
		case "CLOSE":
			return p.parseCloseStmt()
		case "DEALLOCATE":
			return p.parseDeallocateStmt()
		case "FETCH":
			return p.parseFetchStmt()
		case "WHILE":
			return p.parseWhileStmt()
		case "PRINT":
			return p.parsePrintStmt()
		}
	}

	return nil, errors.New("expected keyword")

}

// parsePrintStmt parses a PRINT statement
func (p *Parser) parsePrintStmt() (Node, error) {
	p.consume() // Consume PRINT

	// PRINT (literal | @variable)
	// i.e. PRINT 'Hello, World!'
	// i.e. PRINT @variable

	// You can print a literal or a @variable
	if p.peek(0).tokenT == AT_TOK {
		variableName := ""
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		variableName = p.peek(0).value.(string) + p.peek(1).value.(string)
		p.consume()
		p.consume()

		return &PrintStmt{
			Expr: &Identifier{Value: variableName},
		}, nil
	} else {
		// check for literal
		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New("expected literal")
		}

		return &PrintStmt{
			Expr: &Literal{Value: p.peek(0).value},
		}, nil
	}

}

// parseWhileStmt parses a WHILE statement
func (p *Parser) parseWhileStmt() (Node, error) {
	// Look for
	// WHILE @@FETCH_STATUS = 0
	// BEGIN

	p.consume() // Consume WHILE

	if p.peek(0).tokenT != AT_TOK {
		return nil, errors.New("expected @")
	}

	p.consume() // Consume @

	if p.peek(0).tokenT != AT_TOK {
		return nil, errors.New("expected @")
	}

	p.consume() // Consume @

	// Should be FETCH_STATUS
	if p.peek(0).tokenT != IDENT_TOK || p.peek(0).value != "FETCH_STATUS" {
		return nil, errors.New("expected FETCH_STATUS")

	}

	p.consume() // Consume FETCH_STATUS

	// Next tok should be =
	if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
		return nil, errors.New("expected =")
	}

	p.consume() // Consume =

	// Next tok should be 0
	if p.peek(0).tokenT != LITERAL_TOK || p.peek(0).value != uint64(0) {
		return nil, errors.New("expected 0") // While @@FETCH_STATUS = 0 means while there are rows to fetch
		// There is no benefit in using -1, or -2, or any other number
	}

	fetchStatus := p.peek(0).value.(uint64)

	p.consume() // Consume 0

	// Next tok should be BEGIN

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "BEGIN" {
		return nil, errors.New("expected BEGIN")
	}

	p.consume() // Consume BEGIN

	// Parse statements inside the while loop
	stmts, err := p.parseCursorStmts()
	if err != nil {
		return nil, err
	}

	// Look for END
	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "END" {
		return nil, errors.New("expected END")
	}

	p.consume() // Consume END

	return &WhileStmt{
		FetchStatus: &Literal{Value: fetchStatus},
		Stmts: &BeginEndBlock{
			Stmts: stmts,
		},
	}, nil

}

// parseCursorStmts parses statements inside a cursor
func (p *Parser) parseCursorStmts() ([]interface{}, error) {
	stmts := make([]interface{}, 0)

	for {
		if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "END" {
			break
		}

		stmt, err := p.Parse()
		if err != nil {
			return nil, err
		}

		stmts = append(stmts, stmt)

		if p.peek(0).tokenT == SEMICOLON_TOK {
			p.consume()
			continue
		}
	}

	return stmts, nil

}

// parseFetchStmt parses a FETCH statement
func (p *Parser) parseFetchStmt() (Node, error) {
	p.consume() // Consume FETCH

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "NEXT" {
		return nil, errors.New("expected NEXT")
	}

	p.consume() // Consume NEXT

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
		return nil, errors.New("expected FROM")
	}

	p.consume() // Consume FROM

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	// check for INTO
	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "INTO" {
		return nil, errors.New("expected INTO")
	}

	p.consume() // Consume INTO

	var into []*Identifier

	for {
		// The cursor variable name must start with a @
		if p.peek(0).tokenT != IDENT_TOK && !strings.HasPrefix(p.peek(0).value.(string), "@") {
			return nil, errors.New("expected cursor variable name")
		}

		varName := p.peek(0).value.(string)

		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		varName += p.peek(1).value.(string)

		p.consume() // Consume cursor variable name
		p.consume()

		if p.peek(0).tokenT != COMMA_TOK {
			into = append(into, &Identifier{Value: varName})
			break
		} else {
			into = append(into, &Identifier{Value: varName})
			p.consume()
		}

	}

	return &FetchStmt{
		CursorName: &Identifier{Value: cursorName},
		Into:       into,
	}, nil

}

// parseDeallocateStmt parses a DEALLOCATE statement
func (p *Parser) parseDeallocateStmt() (Node, error) {
	p.consume() // Consume DEALLOCATE

	if p.peek(0).tokenT != IDENT_TOK && p.peek(0).value != "@" {
		return nil, errors.New("expected identifier")
	}

	// if the ident starts with a @
	if strings.HasPrefix(p.peek(0).value.(string), "@") {
		// check next token
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		variableName := p.peek(0).value.(string) + p.peek(1).value.(string)
		p.consume()
		p.consume()

		return &DeallocateStmt{
			CursorVariableName: &Identifier{Value: variableName},
		}, nil

	} else {

		cursorName := p.peek(0).value.(string)
		p.consume() // Consume cursor name

		return &DeallocateStmt{
			CursorName: &Identifier{Value: cursorName},
		}, nil
	}

}

// parseCloseStmt parses an CLOSE statement
func (p *Parser) parseCloseStmt() (Node, error) {
	p.consume() // Consume OPEN

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	return &CloseStmt{
		CursorName: &Identifier{Value: cursorName},
	}, nil

}

// parseOpenStmt parses an OPEN statement
func (p *Parser) parseOpenStmt() (Node, error) {
	p.consume() // Consume OPEN

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	cursorName := p.peek(0).value.(string)
	p.consume() // Consume cursor name

	return &OpenStmt{
		CursorName: &Identifier{Value: cursorName},
	}, nil

}

// parseDeclareStmt parses a DECLARE statement
func (p *Parser) parseDeclareStmt() (Node, error) {
	p.consume() // Consume DECLARE

	if p.peek(0).tokenT != IDENT_TOK && p.peek(0).value != "@" {
		return nil, errors.New("expected identifier")
	}

	// if the ident starts with a @
	if strings.HasPrefix(p.peek(0).value.(string), "@") {
		if p.peek(1).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		// we know it's a cursor variable not a cursor
		cursorVariableName := p.peek(0).value.(string) + p.peek(1).value.(string)

		// consume the cursor variable name
		p.consume()

		p.consume()

		// Check for datatype
		if p.peek(0).tokenT != DATATYPE_TOK {
			return nil, errors.New("expected datatype")
		}

		// consume the datatype
		cursorVariableDataType := p.peek(0).value.(string)

		// consume the datatype
		p.consume()

		return &DeclareStmt{
			CursorVariableName: &Identifier{Value: cursorVariableName},
			CursorVariableDataType: &Identifier{
				Value: cursorVariableDataType,
			},
		}, nil

	} else {
		// we know it's a cursor
		cursorName := p.peek(0).value.(string)

		// consume the cursor name
		p.consume()

		// look gor CURSOR FOR
		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "CURSOR" {
			return nil, errors.New("expected CURSOR")
		}

		p.consume() // Consume CURSOR

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FOR" {
			return nil, errors.New("expected FOR")
		}

		p.consume() // Consume FOR

		// parse select statement
		selectStmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}

		return &DeclareStmt{
			CursorName: &Identifier{Value: cursorName},
			CursorStmt: selectStmt.(*SelectStmt),
		}, nil

	}

}

// parseAlterStmt parses an ALTER statement
func (p *Parser) parseAlterStmt() (Node, error) {
	p.consume() // Consume ALTER

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "USER":
		return p.parseAlterUserStmt()
		//case "TABLE":
		//	return p.parseAlterTableStmt()
	}

	return nil, errors.New("expected USER or TABLE")

}

// parseAlterUserStmt parses an ALTER USER statement
func (p *Parser) parseAlterUserStmt() (Node, error) {
	alterUserStmt := &AlterUserStmt{}
	p.consume() // Consume USER

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	alterUserStmt.Username = &Identifier{Value: p.peek(0).value.(string)}
	p.consume() // Consume username

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "SET":
		p.consume() // Consume SET
		switch p.peek(0).value {
		case "PASSWORD":
			alterUserStmt.SetType = ALTER_USER_SET_PASSWORD
		case "USERNAME":
			alterUserStmt.SetType = ALTER_USER_SET_USERNAME
		default:
			return nil, errors.New("expected PASSWORD or USERNAME")

		}
	default:
		return nil, errors.New("expected SET")
	}

	p.consume() // Consume PASSWORD or USERNAME

	if p.peek(0).tokenT != LITERAL_TOK {

		return nil, errors.New("expected literal")
	}

	alterUserStmt.Value = &Literal{Value: strings.TrimSuffix(strings.TrimPrefix(p.peek(0).value.(string), "'"), "'")}

	return alterUserStmt, nil
}

// parseShowStmt parses a SHOW statement
func (p *Parser) parseShowStmt() (Node, error) {
	p.consume() // Consume SHOW

	switch strings.ToUpper(p.peek(0).value.(string)) {
	case "DATABASES":
		return &ShowStmt{ShowType: SHOW_DATABASES}, nil
	case "TABLES":
		return &ShowStmt{ShowType: SHOW_TABLES}, nil
	case "USERS":
		return &ShowStmt{ShowType: SHOW_USERS}, nil
	case "INDEXES":
		p.consume() // Consume INDEXES

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
			return nil, errors.New("expected FROM")
		}

		p.consume() // Consume FROM

		if p.peek(0).tokenT != IDENT_TOK {

			return nil, errors.New("expected identifier")
		}

		tableName := p.peek(0).value.(string)

		return &ShowStmt{ShowType: SHOW_INDEXES, From: &Identifier{
			Value: tableName,
		}}, nil
	case "GRANTS":

		p.consume() // Consume GRANTS

		if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "FOR" {
			p.consume() // Consume ON

			if p.peek(0).tokenT != IDENT_TOK {
				return nil, errors.New("expected identifier")
			}

			tableName := p.peek(0).value.(string)
			p.consume() // Consume username

			return &ShowStmt{ShowType: SHOW_GRANTS, For: &Identifier{Value: tableName}}, nil
		}

		return &ShowStmt{ShowType: SHOW_GRANTS}, nil
	}

	return nil, errors.New("expected DATABASES, TABLES, or USERS")

}

// parseRevokeStmt parses a REVOKE statement
func (p *Parser) parseRevokeStmt() (Node, error) {
	p.consume() // Consume REVOKE

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL", "DROP", "CREATE", "CONNECT", "ALTER":
		return p.parsePrivilegeStmt(true)
	}

	return nil, errors.New("expected SELECT, INSERT, UPDATE, DELETE")

}

// parseGrantStmt parses a GRANT statement
func (p *Parser) parseGrantStmt() (Node, error) {

	p.consume() // Consume GRANT

	if p.peek(0).tokenT != KEYWORD_TOK {
		return nil, errors.New("expected keyword")
	}

	switch p.peek(0).value {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "ALL", "DROP", "CREATE", "CONNECT", "ALTER":
		return p.parsePrivilegeStmt(false)
	}

	return nil, errors.New("expected SELECT, INSERT, UPDATE, DELETE")

}

// parsePrivilegeStmt parses a privilege statement
func (p *Parser) parsePrivilegeStmt(revoke bool) (Node, error) {
	//  GRANT SELECT, INSERT, UPDATE, DELETE ON database.table TO user;

	grantStmt := &GrantStmt{}
	revokeStmt := &RevokeStmt{}

	privilegeDefinition := &PrivilegeDefinition{
		Actions: make([]shared.PrivilegeAction, 0),
	}

	all := false

	for {
		switch p.peek(0).value {
		case "ALL":
			privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_ALL)
			all = true
		case "SELECT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_SELECT)
			}
		case "BEGIN":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_BEGIN)
			}
		case "COMMIT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_COMMIT)
			}
		case "ROLLBACK":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_ROLLBACK)
			}
		case "INSERT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_INSERT)
			}
		case "UPDATE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_UPDATE)
			}
		case "DELETE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_DELETE)
			}
		case "DROP":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_DROP)
			}
		case "CREATE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_CREATE)
			}
		case "CONNECT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_CONNECT)
			}
		case "ALTER":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_ALTER)
			}
		case "REVOKE":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_REVOKE)
			}
		case "GRANT":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_GRANT)
			}
		case "SHOW":
			if !all {
				privilegeDefinition.Actions = append(privilegeDefinition.Actions, shared.PRIV_SHOW)
			}

		default:
			return nil, errors.New("expected SELECT, INSERT, UPDATE, DELETE, ALL, DROP, CREATE, CONNECT, ALTER")
		}

		p.consume()

		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		} else {
			break
		}

	}

	if p.peek(0).value != "TO" {

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "ON" {
			return nil, errors.New("expected ON")
		}

		p.consume() // Consume ON

		if p.peek(0).tokenT != IDENT_TOK {
			if p.peek(0).value != "*" {
				return nil, errors.New("expected identifier or * ")
			}
		}

		db := &Identifier{}
		table := &Identifier{}

		// if asterisk is found, set database to *
		if p.peek(0).value == "*" {
			db = &Identifier{Value: "*"}
			p.consume()

			// check if next value is * or identifier
			if p.peek(0).value == ".*" {
				table = &Identifier{Value: "*"}
			}
			p.consume() // Consume table name
			privilegeDefinition.Object = &Identifier{Value: db.Value + "." + table.Value}

		} else {
			if len(strings.Split(p.peek(0).value.(string), ".")) != 2 {
				return nil, errors.New("expected database.table, *.*, or database.*")
			}

			db = &Identifier{Value: strings.Split(p.peek(0).value.(string), ".")[0]}
			table = &Identifier{Value: strings.Split(p.peek(0).value.(string), ".")[1]}

			privilegeDefinition.Object = &Identifier{Value: db.Value + "." + table.Value}

			p.consume() // Consume table name

		}

		if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "TO" {
			return nil, errors.New("expected TO")
		}
	}

	p.consume() // Consume TO

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	user := p.peek(0).value.(string)

	if revoke {
		privilegeDefinition.Revokee = &Identifier{Value: user}
		revokeStmt.PrivilegeDefinition = privilegeDefinition

		return revokeStmt, nil

	}
	privilegeDefinition.Grantee = &Identifier{Value: user}
	grantStmt.PrivilegeDefinition = privilegeDefinition

	return grantStmt, nil

}

// parseBeginStmt parses a BEGIN statement
func (p *Parser) parseBeginStmt() (Node, error) {
	p.consume() // Consume BEGIN
	return &BeginStmt{}, nil
}

// parseCommitStmt parses a COMMIT statement
func (p *Parser) parseCommitStmt() (Node, error) {
	p.consume() // Consume COMMIT
	return &CommitStmt{}, nil
}

// parseRollbackStmt parses a ROLLBACK statement
func (p *Parser) parseRollbackStmt() (Node, error) {
	p.consume() // Consume ROLLBACK
	return &RollbackStmt{}, nil

}

// parseDeleteStmt parses a DELETE statement
func (p *Parser) parseDeleteStmt() (Node, error) {
	p.consume() // Consume DELETE

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "FROM" {
		return nil, errors.New("expected FROM")
	}

	p.consume() // Consume FROM

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)
	p.consume() // Consume table name

	deleteStmt := &DeleteStmt{
		TableName: &Identifier{Value: tableName},
	}

	if p.peek(0).tokenT == KEYWORD_TOK && p.peek(0).value == "WHERE" {
		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		deleteStmt.WhereClause = whereClause
	}

	return deleteStmt, nil

}

// parseUpdateStmt parses an UPDATE statement
func (p *Parser) parseUpdateStmt() (Node, error) {
	p.consume() // Consume UPDATE

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	tableName := p.peek(0).value.(string)
	p.consume() // Consume table name

	if p.peek(0).tokenT != KEYWORD_TOK || p.peek(0).value != "SET" {
		return nil, errors.New("expected SET")
	}

	p.consume() // Consume SET

	updateStmt := &UpdateStmt{
		TableName: &Identifier{Value: tableName},
		SetClause: make([]*SetClause, 0),
	}

	for p.peek(0).value != "WHERE" {

		if p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected identifier")
		}

		columnName := p.peek(0).value.(string)
		p.consume() // Consume column name

		if p.peek(0).tokenT != COMPARISON_TOK || p.peek(0).value != "=" {
			return nil, errors.New("expected =")
		}

		p.consume() // Consume =

		var literal interface{}

		if p.peek(0).tokenT != LITERAL_TOK {
			if p.peek(0).value == "SYS_DATE" {
				literal = &shared.SysDate{}
			} else if p.peek(0).value == "SYS_TIME" {
				literal = &shared.SysTime{}
			} else if p.peek(0).value == "GENERATE_UUID" {
				literal = &shared.GenUUID{}
			} else if p.peek(0).value == "SYS_TIMESTAMP" {
				literal = &shared.SysTimestamp{}
			} else if p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == DIVIDE_TOK {
				// binary expression
				binaryExpr, err := p.parseBinaryExpr(0)
				if err != nil {
					return nil, err
				}

				literal = binaryExpr

				p.rewind(1)
			} else if p.peek(0).tokenT == IDENT_TOK {
				literal = &Identifier{Value: p.peek(0).value.(string)}
			}
		} else if p.peek(0).tokenT == LITERAL_TOK {
			literal = p.peek(0).value
		}

		setClause := &SetClause{
			Column: &Identifier{Value: columnName},
			Value:  &Literal{Value: literal},
		}

		updateStmt.SetClause = append(updateStmt.SetClause, setClause)

		p.consume()

		if p.peek(0).tokenT == SEMICOLON_TOK {
			break
		} else if p.peek(0).tokenT != COMMA_TOK {
			break
		}

	}

	// Parse where
	if p.peek(0).tokenT == KEYWORD_TOK || p.peek(0).value == "WHERE" {
		whereClause, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}

		updateStmt.WhereClause = whereClause
	}

	return updateStmt, nil

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
	case "USER":
		return p.parseDropUserStmt()
	}

	return nil, errors.New("expected DATABASE or TABLE")

}

// parseDropUserStmt parses a DROP USER statement
func (p *Parser) parseDropUserStmt() (Node, error) {
	p.consume() // Consume USER

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	user := p.peek(0).value.(string)
	p.consume() // Consume user

	return &DropUserStmt{
		Username: &Identifier{Value: user},
	}, nil

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
	insertStmt.Values = make([][]interface{}, 0)

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

		values := make([]interface{}, 0)

		for {
			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).value != "NULL" && p.peek(0).value != "SYS_DATE" && p.peek(0).value != "SYS_TIME" && p.peek(0).value != "SYS_TIMESTAMP" && p.peek(0).value != "GENERATE_UUID" {

				return nil, errors.New("expected literal or NULL")

			}

			if p.peek(0).value == "NULL" {
				values = append(values, &Literal{Value: nil})
			} else if p.peek(0).value == "SYS_DATE" {
				values = append(values, &shared.SysDate{})
			} else {
				values = append(values, &Literal{Value: p.peek(0).value})
			}

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

	switch strings.ToUpper(p.peek(0).value.(string)) {
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
	case "USER":
		return p.parseCreateUserStmt()
	}

	return nil, errors.New("expected DATABASE or TABLE or INDEX")

}

// parseCreateUserStmt
func (p *Parser) parseCreateUserStmt() (Node, error) {
	createUserStmt := &CreateUserStmt{}

	// Eat USER
	p.consume()

	if p.peek(0).tokenT != IDENT_TOK {
		return nil, errors.New("expected identifier")
	}

	username := p.peek(0).value.(string)
	createUserStmt.Username = &Identifier{Value: username}

	p.consume() // Consume username

	// Eat IDENTIFIED
	if p.peek(0).value != "IDENTIFIED" {
		return nil, errors.New("expected IDENTIFIED")
	}

	p.consume() // Consume IDENTIFIED

	// Eat BY
	if p.peek(0).value != "BY" {
		return nil, errors.New("expected BY")
	}

	p.consume() // Consume BY

	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New("expected literal")
	}

	password := p.peek(0).value.(string)
	createUserStmt.Password = &Literal{Value: strings.TrimSuffix(strings.TrimPrefix(password, "'"), "'")}

	p.consume() // Consume password

	return createUserStmt, nil

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

			err := p.parseTableConstraints(createTableStmt, "")
			if err != nil {
				return nil, err
			}
			break
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

			case "CHAR", "CHARACTER", "BINARY":
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

		err := p.parseTableConstraints(createTableStmt, columnName)
		if err != nil {
			return nil, err

		}

	}

	p.consume() // Consume ,

	return createTableStmt, nil
}

func (p *Parser) parseTableConstraints(createTableStmt *CreateTableStmt, columnName string) error {
	// Check for constraints
	if p.peek(0).tokenT == KEYWORD_TOK {
		for p.peek(0).tokenT == KEYWORD_TOK {
			switch p.peek(0).value {
			case "CHECK":
				p.consume() // Consume CHECK

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New("expected (")
				}

				p.consume() // Consume (

				searchCond, err := p.parseSearchCondition()
				if err != nil {
					return err
				}

				createTableStmt.TableSchema.ColumnDefinitions[columnName].Check = searchCond

				if p.peek(0).tokenT != RPAREN_TOK {
					return errors.New("expected )")
				}

				p.consume() // Consume )

			case "DEFAULT":
				p.consume() // Consume DEFAULT

				if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != KEYWORD_TOK {
					return errors.New("expected literal or keyword")
				}

				defaultValue := p.peek(0).value

				if defaultValue == "SYS_DATE" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = &shared.SysDate{}
				} else if defaultValue == "SYS_TIME" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = &shared.SysTime{}
				} else if defaultValue == "SYS_TIMESTAMP" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = &shared.SysTimestamp{}
				} else if defaultValue == "GENERATE_UUID" {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = &shared.GenUUID{}
				} else {
					createTableStmt.TableSchema.ColumnDefinitions[columnName].Default = &Literal{Value: defaultValue}
				}

				p.consume() // Consume literal or keyword

			case "PRIMARY":
				p.consume() // Consume PRIMARY
				if p.peek(0).value != "KEY" {
					return errors.New("expected KEY")
				}

				p.consume() // Consume KEY
				// We set not null to true
				// We set unique to true
				// We set sequence to true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].NotNull = true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = true
			case "FOREIGN":
				p.consume() // Consume FOREIGN
				// FOREIGN KEY (1) REFERENCES Departments(2)
				// 1, 2 should be the same column identifier
				if p.peek(0).value != "KEY" {
					return errors.New("expected KEY")
				}

				p.consume() // Consume KEY

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New("expected (")
				}

				p.consume() // Consume (

				refColumn := ""

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New("expected identifier")
				}

				refColumn = p.peek(0).value.(string)

				p.consume() // Consume column name

				if p.peek(0).tokenT != RPAREN_TOK {
					return errors.New("expected )")
				}

				p.consume() // Consume )

				// Check for REFERENCES keyword
				if p.peek(0).value != "REFERENCES" {
					return errors.New("expected REFERENCES")
				}

				p.consume() // Consume REFERENCES

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New("expected identifier")
				}

				refTable := p.peek(0).value.(string)

				p.consume() // Consume table name

				createTableStmt.TableSchema.ColumnDefinitions[refColumn].References = &catalog.Reference{
					ColumnName: refColumn,
					TableName:  refTable,
				}

				if p.peek(0).tokenT != LPAREN_TOK {
					return errors.New("expected (")
				}

				p.consume() // Consume (

				if p.peek(0).tokenT != IDENT_TOK {
					return errors.New("expected identifier")
				}

				// Check if the column name is the same as the reference column name
				if p.peek(0).value != refColumn {
					return errors.New("expected column name to be the same as the reference column name")
				}

			case "NOT":
				p.consume() // Consume NOT

				if p.peek(0).value != "NULL" {
					return errors.New("expected NULL")
				}

				p.consume() // Consume NULL

				createTableStmt.TableSchema.ColumnDefinitions[columnName].NotNull = true
			case "UNIQUE":
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Unique = true

				p.consume() // Consume UNIQUE
			case "SEQUENCE":
				createTableStmt.TableSchema.ColumnDefinitions[columnName].Sequence = true

				p.consume() // Consume SEQUENCE
			default:
				return errors.New("expected NOT NULL or UNIQUE or SEQUENCE")
			}

		}
	}

	p.consume()

	return nil

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

	// Look for GROUP BY
	if p.peek(0).value == "GROUP" {
		if p.peek(1).value != "BY" {
			return nil, errors.New("expected BY")

		} else {
			p.consume()
			p.consume()

			groupByClause, err := p.parseGroupByClause()
			if err != nil {
				return nil, err
			}

			selectStmt.TableExpression.GroupByClause = groupByClause

		}
	}

	if p.peek(0).value == "HAVING" {
		havingClause, err := p.parseHavingClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.HavingClause = havingClause

	}

	// Look for ORDER BY
	if p.peek(0).value == "ORDER" {
		if p.peek(1).value != "BY" {
			return nil, errors.New("expected BY")

		} else {

			orderByClause, err := p.parseOrderByClause()
			if err != nil {
				return nil, err
			}

			selectStmt.TableExpression.OrderByClause = orderByClause

		}

	}

	if p.peek(0).value == "LIMIT" {
		limitClause, err := p.parseLimitClause()
		if err != nil {
			return nil, err
		}

		selectStmt.TableExpression.LimitClause = limitClause
	}

	// Look for union
	if p.peek(0).value == "UNION" {
		p.consume()
		if p.peek(0).value == "ALL" {
			selectStmt.UnionAll = true
			p.consume()
		}

		unionStmt, err := p.parseSelectStmt()
		if err != nil {
			return nil, err
		}

		selectStmt.Union = unionStmt.(*SelectStmt)

	}

	return selectStmt, nil

}

// parseLimitClause parses a LIMIT clause
func (p *Parser) parseLimitClause() (*LimitClause, error) {
	limitClause := &LimitClause{}

	// Eat LIMIT
	p.consume()

	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New("expected literal")
	}

	count := p.peek(0).value.(uint64)

	p.consume()

	// check for offset
	if p.peek(0).value == "OFFSET" {
		p.consume()

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New("expected literal")
		}

		offset := p.peek(0).value.(uint64)
		limitClause.Offset = &Literal{Value: offset}

		p.consume()
	}

	limitClause.Count = &Literal{Value: count}

	return limitClause, nil

}

// parseOrderByClause parses an ORDER BY clause
func (p *Parser) parseOrderByClause() (*OrderByClause, error) {
	orderByClause := &OrderByClause{}

	// Parse order by list
	err := p.parseOrderByList(orderByClause)
	if err != nil {
		return nil, err
	}

	return orderByClause, nil
}

// parseOrderByList parses an order by list
func (p *Parser) parseOrderByList(orderByClause *OrderByClause) error {
	p.consume() // Consume ORDER

	if p.peek(0).value != "BY" {
		return errors.New("expected BY")
	}

	p.consume() // Consume BY

	for p.peek(0).tokenT != EOF_TOK || p.peek(0).value != "ROWS" || p.peek(0).value == "RANGE" || p.peek(0).tokenT != RPAREN_TOK {

		// Parse order by expression
		expr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		orderByClause.OrderByExpressions = append(orderByClause.OrderByExpressions, expr)

		// Look for ,
		if p.peek(0).value == "," {
			p.consume() // Consume ,

			continue
		}

		break

	}

	if p.peek(0).value == "ASC" {
		orderByClause.Order = ASC
		p.consume()

	} else {
		orderByClause.Order = DESC
		p.consume()
	}

	return nil

}

// parseGroupByClause
func (p *Parser) parseGroupByClause() (*GroupByClause, error) {

	groupByClause := &GroupByClause{}

	// Parse group by list
	err := p.parseGroupByList(groupByClause)
	if err != nil {
		return nil, err
	}

	return groupByClause, nil
}

// parseGroupByList parses a group by list
func (p *Parser) parseGroupByList(groupByClause *GroupByClause) error {
	// Parse group by expression
	expr, err := p.parseValueExpression()
	if err != nil {
		return err
	}

	groupByClause.GroupByExpressions = append(groupByClause.GroupByExpressions, expr)

	// Look for ,
	for p.peek(0).value == "," {
		p.consume() // Consume ,

		expr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		groupByClause.GroupByExpressions = append(groupByClause.GroupByExpressions, expr)
	}

	return nil
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

	var expr interface{}
	var err error
	var not *NotExpr

	if p.peek(0).tokenT == IDENT_TOK {
		if p.peek(1).value == "NOT" {
			// put ident in the not position

			p.switchToken(0, 1)

			p.consume()

			not = &NotExpr{}
		}

	}

	if p.peek(0).value == "EXISTS" {
		// Parse subquery

		p.consume()
		p.consume()

		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		return &ExistsPredicate{
			Expr: subquery,
		}, nil
	}

	if p.peek(1).tokenT == COMPARISON_TOK || p.peek(1).tokenT == ASTERISK_TOK || p.peek(1).tokenT == PLUS_TOK || p.peek(1).tokenT == MINUS_TOK || p.peek(1).tokenT == DIVIDE_TOK || p.peek(1).tokenT == MODULUS_TOK || p.peek(1).tokenT == AT_TOK {
		// Parse comparison expression
		expr, err = p.parseComparisonExpr(nil)
		if err != nil {
			return nil, err
		}

	} else if p.peek(1).tokenT == KEYWORD_TOK {
		// Check for case
		if p.peek(0).value == "CASE" {
			expr, err = p.parseCaseExpr()
			if err != nil {
				return nil, err
			}

			// Check next token
			// Case can be evaluated as a left side of a predicate
			if p.peek(0).tokenT == COMPARISON_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == DIVIDE_TOK || p.peek(0).tokenT == MODULUS_TOK || p.peek(0).tokenT == AT_TOK {
				// Parse comparison expression
				expr, err = p.parseComparisonExpr(&ValueExpression{
					Value: expr,
				})
				if err != nil {
					return nil, err
				}
			} else if p.peek(0).tokenT == KEYWORD_TOK {
				switch p.peek(0).value {
				case "BETWEEN":
					// Parse between expression
					expr, err = p.parseBetweenExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "IN":
					// Parse in expression
					expr, err = p.parseInExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "LIKE":
					// Parse like expression
					expr, err = p.parseLikeExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}

					if not != nil {
						not.Expr = expr
						expr = not
					}
				case "IS":
					// Parse is expression
					expr, err = p.parseIsExpr(&ValueExpression{
						Value: expr,
					})
					if err != nil {
						return nil, err
					}
				default:
					return nil, errors.New("expected predicate or logical expression")

				}
			}

		}

		switch p.peek(1).value {
		case "BETWEEN":

			// Parse between expression
			expr, err = p.parseBetweenExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}

		case "IN":
			// Parse in expression
			expr, err = p.parseInExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "LIKE":
			// Parse like expression
			expr, err = p.parseLikeExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "IS":
			// Parse is expression
			expr, err = p.parseIsExpr(nil)
			if err != nil {
				return nil, err
			}

		}
	} else if p.peek(0).tokenT == KEYWORD_TOK {

		currentPos := p.pos

		if p.peek(0).value == "AVG" || p.peek(0).value == "COUNT" || p.peek(0).value == "MAX" || p.peek(0).value == "MIN" || p.peek(0).value == "SUM" {
			expr, err = p.parseAggregateFunc()
			if err != nil {
				return nil, err
			}
		} else if p.peek(0).value == "LENGTH" || p.peek(0).value == "LOWER" || p.peek(0).value == "UPPER" || p.peek(0).value == "TRIM" || p.peek(0).value == "SUBSTRING" || p.peek(0).value == "POSITION" || p.peek(0).value == "CONCAT" || p.peek(0).value == "COALESCE" ||
			p.peek(0).value == "CAST" || p.peek(0).value == "REVERSE" || p.peek(0).value == "ROUND" || p.peek(0).value == "REPLACE" || p.peek(0).value == "TRIM" || p.peek(0).value == "COALESCE" {
			expr, err = p.parseSystemFunc()
			if err != nil {
				return nil, err
			}
		}

		if p.peek(0).tokenT == COMPARISON_TOK || p.peek(0).tokenT == ASTERISK_TOK || p.peek(0).tokenT == PLUS_TOK || p.peek(0).tokenT == MINUS_TOK || p.peek(0).tokenT == DIVIDE_TOK || p.peek(0).tokenT == MODULUS_TOK || p.peek(0).tokenT == AT_TOK {
			// Parse comparison expression
			p.pos = currentPos

			expr, err = p.parseComparisonExpr(nil)
			if err != nil {
				return nil, err
			}

		}

		switch p.peek(0).value {
		case "BETWEEN":

			// Parse between expression
			expr, err = p.parseBetweenExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}

		case "IN":
			// Parse in expression
			expr, err = p.parseInExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "LIKE":
			// Parse like expression
			expr, err = p.parseLikeExpr(nil)
			if err != nil {
				return nil, err
			}

			if not != nil {
				not.Expr = expr
				expr = not
			}
		case "IS":
			// Parse is expression
			expr, err = p.parseIsExpr(nil)
			if err != nil {
				return nil, err
			}

		}
	} else {
		return nil, errors.New("expected predicate or logical expression")
	}

	if p.peek(0).tokenT == KEYWORD_TOK {
		if p.peek(0).value == "AND" || p.peek(0).value == "OR" {
			// Parse logical expression
			expr, err = p.parseLogicalExpr(expr)
			if err != nil {
				return nil, err
			}

		}
	}

	return expr, nil

}

// parseLikeExpr parses a LIKE expression
func (p *Parser) parseLikeExpr(left *ValueExpression) (*LikePredicate, error) {
	// Parse left side of like expression

	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat LIKE
	p.consume()

	// Parse pattern
	pattern, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &LikePredicate{
		Left:    left,
		Pattern: pattern,
	}, nil

}

// parseIsExpr parses an IS expression
func (p *Parser) parseIsExpr(left *ValueExpression) (*IsPredicate, error) {
	// Parse left side of is expression
	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat IS
	p.consume()

	// NULL or NOT NULL

	if p.peek(0).value == "NULL" {
		p.consume()
		return &IsPredicate{
			Left: left,
			Null: true,
		}, nil
	} else if p.peek(0).value == "NOT" {
		// Eat NOT
		p.consume()

		if p.peek(0).value != "NULL" {
			return nil, errors.New("expected NULL")
		}

		p.consume()

		return &IsPredicate{
			Left: left,
			Null: false,
		}, nil

	}

	return nil, errors.New("expected NULL or NOT NULL")

}

// parseInExpr parses an IN expression
func (p *Parser) parseInExpr(left *ValueExpression) (*InPredicate, error) {
	// Parse left side of in expression

	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat IN
	p.consume()

	// Eat (
	p.consume()

	inPredicate := &InPredicate{
		Left: left,
	}

	if p.peek(0).value == "SELECT" {
		// Parse subquery
		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		inPredicate.Values = append(inPredicate.Values, subquery)

		// Eat )
		p.consume()

		return inPredicate, nil

	}

	for p.peek(0).tokenT != EOF_TOK {
		if p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		}

		// Parse right side of in expression
		right, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		inPredicate.Values = append(inPredicate.Values, right)
	}

	// Eat )
	p.consume()

	return inPredicate, nil

}

// parseBetweenExpr parses a between expression
func (p *Parser) parseBetweenExpr(left *ValueExpression) (*BetweenPredicate, error) {
	// check for not if there remove

	if left == nil {
		var err error
		// Parse left side of between expression
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
	}

	// Eat BETWEEN
	p.consume()

	// Parse lower bound
	lower, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	// Eat AND
	p.consume()

	// Parse upper bound
	upper, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	return &BetweenPredicate{
		Left:  left,
		Lower: lower,
		Upper: upper,
	}, nil

}

// parseComparisonExpr parses a comparison expression
func (p *Parser) parseComparisonExpr(left *ValueExpression) (*ComparisonPredicate, error) {
	// Parse left side of comparison
	if left == nil {
		var err error
		left, err = p.parseValueExpression()
		if err != nil {
			return nil, err
		}
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
func (p *Parser) parseLogicalExpr(left interface{}) (*LogicalCondition, error) {

	// Parse logical operator
	op := p.peek(0).value.(string)
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

	for p.peek(0).tokenT != SEMICOLON_TOK || p.peek(0).value != "WHERE" || p.peek(0).value != "INNER" || p.peek(0).value != "LEFT" || p.peek(0).value != "RIGHT" || p.peek(0).value != "FULL" || p.peek(0).value != "GROUP" || p.peek(0).value != "HAVING" || p.peek(0).value != "ORDER" || p.peek(0).value != "LIMIT" || p.peek(0).value != "UNION" || p.peek(0).value != "JOIN" {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()

			continue
		}

		if p.peek(0).tokenT == SEMICOLON_TOK || p.peek(0).value == "WHERE" || p.peek(0).tokenT == LPAREN_TOK || p.peek(0).tokenT == RPAREN_TOK || p.peek(0).value == "GROUP" || p.peek(0).value == "HAVING" || p.peek(0).value == "ORDER" || p.peek(0).value == "LIMIT" || p.peek(0).value == "INNER" || p.peek(0).value == "LEFT" || p.peek(0).value == "RIGHT" || p.peek(0).value == "FULL" || p.peek(0).value == "GROUP" || p.peek(0).value == "HAVING" || p.peek(0).value == "ORDER" || p.peek(0).value == "LIMIT" || p.peek(0).value == "UNION" || p.peek(0).value == "JOIN" || p.peek(0).value == "JOIN" {
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

	// can have tablename aliasname i.e users u
	// OR tablename aliasname i.e users as u
	if p.peek(0).tokenT == KEYWORD_TOK {
		if p.peek(0).value == "AS" {
			p.consume()
		}
	}

	if p.peek(0).tokenT == IDENT_TOK {
		aliasName, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		table.Alias = aliasName

	}

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

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: expr,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: expr,
		}, nil
	}

	if p.peek(0).tokenT == LPAREN_TOK {
		// Subquery
		p.consume()
		subquery, err := p.parseSubquery()
		if err != nil {
			return nil, err
		}

		// Eat )

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: subquery,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: subquery,
		}, nil
	}

	switch p.peek(0).tokenT {

	case LITERAL_TOK:
		lit, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			alias, err = p.parseIdentifier()
			if err != nil {
				return nil, err
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: lit,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: lit,
		}, nil

	case KEYWORD_TOK:
		switch p.peek(0).value {
		case "COUNT", "MAX", "MIN", "SUM", "AVG",
			"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
			"LEAD", "LAG", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
			"PERCENT_RANK", "CUME_DIST", "PERCENTILE_CONT", "PERCENTILE_DISC":
			expr, err := p.parseBinaryExpr(0)
			if err != nil {
				return nil, err
			}

			if p.peek(0).value == "OVER" {

				// Parse window function
				expr, err = p.parseWindowFunc(expr)
				if err != nil {
					return nil, err
				}
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}
			if alias != nil {
				return &ValueExpression{
					Value: expr,
					Alias: alias,
				}, nil
			}

			return &ValueExpression{
				Value: expr,
			}, nil
		case "CASE":
			caseExpr, err := p.parseCaseExpr()
			if err != nil {
				return nil, err
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}

			if alias != nil {
				return &ValueExpression{
					Value: caseExpr,
					Alias: alias,
				}, nil
			} else {
				return &ValueExpression{
					Value: caseExpr,
				}, nil
			}

		case "UPPER", "LOWER", "CAST",
			"COALESCE", "REVERSE", "ROUND", "POSITION", "LENGTH", "REPLACE", "CONCAT",
			"SUBSTRING", "TRIM", "SYS_DATE", "SYS_TIME", "SYS_TIMESTAMP":
			// Parse system function
			sysFunc, err := p.parseSystemFunc()
			if err != nil {
				return nil, err
			}

			var alias *Identifier

			// Check for alias
			if p.peek(0).value == "AS" {
				p.consume()

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}

			if alias != nil {
				return &ValueExpression{
					Value: sysFunc,
					Alias: alias,
				}, nil
			} else {
				return &ValueExpression{
					Value: sysFunc,
				}, nil
			}
		default:
			return nil, errors.New("expected keyword")
		}

	case AT_TOK:
		if p.peek(1).tokenT == IDENT_TOK {
			p.lexer.tokens[p.pos+1] = Token{
				tokenT: IDENT_TOK,
				value:  "@" + p.peek(1).value.(string),
			}
		}

		p.consume()
		variableName := p.peek(0).value.(string)
		p.consume()
		return &ValueExpression{
			Value: &Variable{
				VariableName: &Identifier{
					Value: variableName,
				},
			},
		}, nil
	case IDENT_TOK:

		// Parse column spec
		colSpec, err := p.parseColumnSpecification()
		if err != nil {
			return nil, err
		}

		var alias *Identifier

		// Check for alias
		if p.peek(0).value == "AS" {
			p.consume()

			// Check if next tok type is a keyword, if so skip alias
			if p.peek(0).tokenT == DATATYPE_TOK {
				p.rewind(1)

				alias = nil

			} else {

				alias, err = p.parseIdentifier()
				if err != nil {
					return nil, err
				}
			}
		}
		if alias != nil {
			return &ValueExpression{
				Value: colSpec,
				Alias: alias,
			}, nil
		}

		return &ValueExpression{
			Value: colSpec,
		}, nil
	default:

		return nil, errors.New("expected column spec or aggregate function or subquery")
	}

}

// parseWindowFunc parses a window function
func (p *Parser) parseWindowFunc(expr interface{}) (*WindowFunc, error) {
	windowFunc := &WindowFunc{}

	// Eat OVER
	p.consume()

	// Eat (
	p.consume()

	// Parse window spec
	windowSpec, err := p.parseWindowSpec()
	if err != nil {
		return nil, err
	}

	windowFunc.Expr = expr
	windowFunc.Spec = windowSpec

	// Eat )
	p.consume()

	return windowFunc, nil

}

// parseWindowSpec parses a window spec
func (p *Parser) parseWindowSpec() (*WindowSpec, error) {
	windowSpec := &WindowSpec{}

	// Parse partition by clause
	err := p.parsePartitionByClause(windowSpec)
	if err != nil {
		return nil, err
	}

	// if next tok is ) then return
	if p.peek(0).tokenT == RPAREN_TOK {
		return windowSpec, nil

	}

	// Parse order by clause
	orderByClause, err := p.parseOrderByClause()
	if err != nil {
		return nil, err
	}

	windowSpec.OrderBy = orderByClause

	// Parse frame clause
	err = p.parseFrameClause(windowSpec)
	if err != nil {
		return nil, err

	}

	return windowSpec, nil

}

// parseFrameClause parses a frame clause
func (p *Parser) parseFrameClause(windowSpec *WindowSpec) error {
	log.Println(p.peek(0).value)

	switch p.peek(0).value {
	case "ROWS":
		// Eat ROWS
		p.consume()

		if p.peek(0).value != "BETWEEN" {
			return errors.New("expected BETWEEN")
		}

		p.consume() // Consume BETWEEN

		if p.peek(0).value == "UNBOUNDED" {
			p.consume() // Consume UNBOUNDED
			if p.peek(0).value == "PRECEDING" {
				p.consume() // Consume PRECEDING

				if p.peek(0).value != "AND" {
					return errors.New("expected AND")
				}

				p.consume() // Consume AND

				if p.peek(0).value == "CURRENT" {
					p.consume() // Consume CURRENT

					if p.peek(0).value != "ROW" {
						return errors.New("expected ROW")
					}

					p.consume() // Consume ROW

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_ROWS,
						Boundary: &WindowFrameBoundary{
							Type: ROWS_UNBOUNDED_PRECEDING_CURRENT_ROW,
						},
					}

					return nil

				} else if p.peek(0).tokenT == LITERAL_TOK {
					upper, err := p.parseLiteral()
					if err != nil {
						return err
					}

					if p.peek(0).value == "FOLLOWING" {
						p.consume() // Consume FOLLOWING

						windowSpec.Frame = &WindowFrame{
							FrameType: WINDOW_FRAME_ROWS,
							Boundary: &WindowFrameBoundary{
								Type:  ROWS_UNBOUNDED_PRECEDING_LITERAL_FOLLOWING,
								Upper: upper.(*Literal),
							},
						}

						return nil
					} else {
						return errors.New("expected FOLLOWING")
					}

				} else if p.peek(0).value == "UNBOUNDED" {
					p.consume() // Consume UNBOUNDED

					if p.peek(0).value != "FOLLOWING" {
						return errors.New("expected FOLLOWING")
					}

					p.consume() // Consume FOLLOWING

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_ROWS,
						Boundary: &WindowFrameBoundary{
							Type: ROWS_UNBOUNDED_PRECEDING_UNBOUNDED_FOLLOWING,
						},
					}

					return nil
				} else {
					return errors.New("expected CURRENT, LITERAL, or UNBOUNDED")
				}

			} else {
				return errors.New("expected PRECEDING")
			}

		} else if p.peek(0).value == "CURRENT" {
			p.consume() // Consume CURRENT

			if p.peek(0).value != "ROW" {
				return errors.New("expected ROW")
			}

			p.consume() // Consume ROW

			if p.peek(0).value != "AND" {
				return errors.New("expected AND")
			}

			p.consume() // Consume AND

			if p.peek(0).value == "UNBOUNDED" {
				p.consume() // Consume UNBOUNDED

				if p.peek(0).value != "FOLLOWING" {
					return errors.New("expected FOLLOWING")
				}

				p.consume() // Consume FOLLOWING

				windowSpec.Frame = &WindowFrame{
					FrameType: WINDOW_FRAME_ROWS,
					Boundary: &WindowFrameBoundary{
						Type: ROWS_CURRENT_ROW_UNBOUNDED_FOLLOWING,
					},
				}

				return nil

			} else if p.peek(0).tokenT == LITERAL_TOK {
				upper, err := p.parseLiteral()
				if err != nil {
					return err
				}

				if p.peek(0).value == "FOLLOWING" {
					p.consume() // Consume FOLLOWING

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_ROWS,
						Boundary: &WindowFrameBoundary{
							Type:  ROWS_CURRENT_ROW_LITERAL_FOLLOWING,
							Upper: upper.(*Literal),
						},
					}

					return nil
				} else {
					return errors.New("expected FOLLOWING")
				}

			} else {
				return errors.New("expected LITERAL or UNBOUNDED")
			}

		} else if p.peek(0).tokenT == LITERAL_TOK {
			lower, err := p.parseLiteral()
			if err != nil {
				return err
			}

			// look for preceding
			if p.peek(0).value == "PRECEDING" {
				p.consume() // Consume PRECEDING

				if p.peek(0).value != "AND" {
					return errors.New("expected AND")
				}

				p.consume() // Consume AND

				if p.peek(0).tokenT == LITERAL_TOK {
					upper, err := p.parseLiteral()
					if err != nil {
						return err
					}

					if p.peek(0).value == "FOLLOWING" {
						p.consume() // Consume FOLLOWING

						windowSpec.Frame = &WindowFrame{
							FrameType: WINDOW_FRAME_ROWS,
							Boundary: &WindowFrameBoundary{
								Type:  ROWS_LITERAL_PRECEDING_LITERAL_FOLLOWING,
								Lower: lower.(*Literal),
								Upper: upper.(*Literal),
							},
						}

						return nil
					} else {
						return errors.New("expected FOLLOWING")
					}

				} else if p.peek(0).value == "CURRENT" {
					p.consume() // Consume CURRENT

					if p.peek(0).value != "ROW" {
						return errors.New("expected ROW")
					}

					p.consume() // Consume ROW

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_ROWS,
						Boundary: &WindowFrameBoundary{
							Type:  ROWS_LITERAL_PRECEDING_CURRENT_ROW,
							Lower: lower.(*Literal),
						},
					}

					return nil
				} else if p.peek(0).value == "UNBOUNDED" {
					p.consume() // Consume UNBOUNDED

					if p.peek(0).value != "FOLLOWING" {
						return errors.New("expected FOLLOWING")
					}

					p.consume() // Consume FOLLOWING

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_ROWS,
						Boundary: &WindowFrameBoundary{
							Type:  ROWS_LITERAL_PRECEDING_UNBOUNDED_FOLLOWING,
							Lower: lower.(*Literal),
						},
					}
				} else {
					return errors.New("expected CURRENT, UNBOUNDED, or LITERAL")
				}

			} else {
				return errors.New("expected PRECEDING")
			}

		}

	case "RANGE":
		// Eat RANGE

		p.consume()

		if p.peek(0).value != "BETWEEN" {
			return errors.New("expected BETWEEN")
		}

		p.consume() // Consume BETWEEN

		if p.peek(0).value == "UNBOUNDED" {
			p.consume()

			if p.peek(0).value == "PRECEDING" {
				p.consume() // Consume PRECEDING

				if p.peek(0).value == "AND" {
					p.consume() // Consume AND

					if p.peek(0).value == "CURRENT" {
						p.consume() // Consume CURRENT

						if p.peek(0).value != "ROW" {
							return errors.New("expected ROW")
						}

						p.consume() // Consume ROW

						windowSpec.Frame = &WindowFrame{
							FrameType: WINDOW_FRAME_RANGE,
							Boundary: &WindowFrameBoundary{
								Type: RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW,
							},
						}

						return nil

					} else if p.peek(0).tokenT == LITERAL_TOK {
						upper, err := p.parseLiteral()
						if err != nil {
							return err
						}

						if p.peek(0).value == "FOLLOWING" {
							p.consume() // Consume FOLLOWING

							windowSpec.Frame = &WindowFrame{
								FrameType: WINDOW_FRAME_RANGE,
								Boundary: &WindowFrameBoundary{
									Type:  RANGE_UNBOUNDED_PRECEDING_LITERAL_FOLLOWING,
									Upper: upper.(*Literal),
								},
							}

							return nil
						} else {
							return errors.New("expected FOLLOWING")
						}

					} else if p.peek(0).value == "UNBOUNDED" {
						p.consume() // Consume UNBOUNDED

						if p.peek(0).value != "FOLLOWING" {
							return errors.New("expected FOLLOWING")
						}

						p.consume() // Consume FOLLOWING

						windowSpec.Frame = &WindowFrame{
							FrameType: WINDOW_FRAME_RANGE,
							Boundary: &WindowFrameBoundary{
								Type: RANGE_UNBOUNDED_PRECEDING_UNBOUNDED_FOLLOWING,
							},
						}

						return nil

					} else {
						return errors.New("expected CURRENT, LITERAL, or UNBOUNDED")
					}
				} else {
					return errors.New("expected AND")
				}

			} else {
				return errors.New("expected PRECEDING")
			}

		} else if p.peek(0).value == "CURRENT" {
			p.consume() // Consume CURRENT

			if p.peek(0).value != "ROW" {
				return errors.New("expected ROW")
			}

			p.consume() // Consume ROW

			if p.peek(0).value != "AND" {
				return errors.New("expected AND")
			}

			p.consume() // Consume AND

			if p.peek(0).value == "UNBOUNDED" {

				p.consume() // Consume UNBOUNDED

				if p.peek(0).value != "FOLLOWING" {
					return errors.New("expected FOLLOWING")
				}

				p.consume() // Consume FOLLOWING

				windowSpec.Frame = &WindowFrame{
					FrameType: WINDOW_FRAME_RANGE,
					Boundary: &WindowFrameBoundary{
						Type: RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING,
					},
				}

				return nil

			} else if p.peek(0).tokenT == LITERAL_TOK {
				upper, err := p.parseLiteral()
				if err != nil {
					return err
				}

				if p.peek(0).value == "FOLLOWING" {
					p.consume() // Consume FOLLOWING

					windowSpec.Frame = &WindowFrame{
						FrameType: WINDOW_FRAME_RANGE,
						Boundary: &WindowFrameBoundary{
							Type:  RANGE_CURRENT_ROW_LITERAL_FOLLOWING,
							Upper: upper.(*Literal),
						},
					}

					return nil
				} else {
					return errors.New("expected FOLLOWING")
				}

			} else {
				return errors.New("expected UNBOUNDED, or LITERAL")
			}

		} else if p.peek(0).tokenT == LITERAL_TOK {

		} else {
			return errors.New("expected UNBOUNDED, CURRENT, or LITERAL")
		}

	}

	return nil

}

// parsePartitionByClause
func (p *Parser) parsePartitionByClause(windowSpec *WindowSpec) error {
	// Eat PARTITION
	p.consume()

	// Eat BY
	p.consume()

	// Parse partition by list
	err := p.parsePartitionByList(windowSpec)
	if err != nil {
		return err
	}

	return nil

}

// parsePartitionByList
func (p *Parser) parsePartitionByList(windowSpec *WindowSpec) error {

	for p.peek(0).value != "ORDER" || p.peek(0).value != "ROWS" || p.peek(0).value != "RANGE" || p.peek(0).value != "GROUPS" || p.peek(0).tokenT != RPAREN_TOK {
		if p.peek(0).tokenT == COMMA_TOK {
			p.consume()
			continue
		}

		if p.peek(0).value == "ORDER" || p.peek(0).value == "ROWS" || p.peek(0).value == "RANGE" || p.peek(0).value == "GROUPS" || p.peek(0).tokenT == RPAREN_TOK {
			break
		}

		// Parse value expression
		expr, err := p.parseValueExpression()
		if err != nil {
			return err
		}

		windowSpec.PartitionBy = append(windowSpec.PartitionBy, expr)

	}

	return nil

}

// parseCaseExpr parses a CASE expression
func (p *Parser) parseCaseExpr() (*CaseExpr, error) {
	caseExpr := &CaseExpr{}

	p.consume() // Consume CASE

	// Parse when clauses
	for p.peek(0).value != "ELSE" {
		whenClause, err := p.parseWhenClause()
		if err != nil {
			return nil, err
		}

		caseExpr.WhenClauses = append(caseExpr.WhenClauses, whenClause)
	}

	// Eat ELSE
	p.consume()

	// Parse else clause
	elseClause, err := p.parseElseClause()
	if err != nil {
		return nil, err
	}

	caseExpr.ElseClause = elseClause

	// Eat END
	p.consume()

	return caseExpr, nil

}

// parseWhenClause parses a WHEN clause
func (p *Parser) parseWhenClause() (*WhenClause, error) {
	whenClause := &WhenClause{}

	p.consume() // Consume WHEN

	// Parse search condition
	searchCondition, err := p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	whenClause.Condition = searchCondition

	// Eat THEN
	p.consume()

	// Parse result
	result, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	whenClause.Result = result

	return whenClause, nil

}

// parseElseClause
func (p *Parser) parseElseClause() (*ElseClause, error) {
	elseClause := &ElseClause{}

	// Parse result
	result, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	elseClause.Result = result

	return elseClause, nil
}

// parseSystemFunction parses system function like UPPER, LOWER, CAST, COALESCE, etc
func (p *Parser) parseSystemFunc() (interface{}, error) {
	switch p.peek(0).value {
	case "SUBSTRING":
		substrFunc := &SubstrFunc{}

		p.consume() // Consume SUBSTRING

		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		p.consume() // Consume (

		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse value expression
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		substrFunc.Arg = expr

		// Look for ,
		if p.peek(0).value != "," {
			return nil, errors.New("expected ,")
		}

		p.consume() // Consume ,

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New("expected literal")
		}

		// Parse literal
		startPos, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		// Look for ,
		if p.peek(0).value != "," {
			return nil, errors.New("expected ,")
		}

		p.consume() // Consume ,

		if p.peek(0).tokenT != LITERAL_TOK {
			return nil, errors.New("expected literal")
		}

		leng, err := p.parseLiteral()
		if err != nil {
			return nil, err

		}

		if p.peek(0).tokenT == RPAREN_TOK {
			p.consume()
		}

		substrFunc.StartPos = startPos.(*Literal)
		substrFunc.Length = leng.(*Literal)

		return substrFunc, nil
	case "CONCAT":
		concatFunc := &ConcatFunc{}

		p.consume() // Consume POSITION

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()
		for p.peek(0).tokenT != RPAREN_TOK || p.peek(0).tokenT != EOF_TOK {

			if p.peek(0).tokenT == RPAREN_TOK {
				break
			}

			// Parse value expression
			expr, err := p.parseValueExpression()
			if err != nil {
				return nil, err
			}

			concatFunc.Args = append(concatFunc.Args, expr)

			// Look for ,
			if p.peek(0).value == "," {
				p.consume() // Consume ,
			}

		}

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		p.consume() // Consume RPAREN

		return concatFunc, nil

	case "POSITION":
		positionFunc := &PositionFunc{}

		p.consume() // Consume POSITION

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err

		}

		// look for IN
		if p.peek(0).value != "IN" {
			return nil, errors.New("expected IN")

		}

		p.consume() // Consume IN

		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		inExpr, err := p.parseValueExpression()
		if err != nil {
			return nil, err

		}

		positionFunc.Arg = expr
		positionFunc.In = inExpr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return positionFunc, nil
	case "LENGTH":
		lengthFunc := &LengthFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		lengthFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return lengthFunc, nil
	case "TRIM":
		trimFunc := &TrimFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		trimFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return trimFunc, nil
	case "ROUND":
		roundFunc := &RoundFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		roundFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return roundFunc, nil
	case "REVERSE":
		reverseFunc := &ReverseFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		reverseFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return reverseFunc, nil
	case "UPPER":
		upperFunc := &UpperFunc{}

		p.consume() // Consume UPPER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		upperFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return upperFunc, nil
	case "LOWER":
		lowerFunc := &LowerFunc{}

		p.consume() // Consume LOWER

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN
		p.consume()

		// Look for literal or identifier
		if p.peek(0).tokenT != LITERAL_TOK && p.peek(0).tokenT != IDENT_TOK {
			return nil, errors.New("expected literal or identifier")
		}

		// Parse literal or identifier
		expr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		lowerFunc.Arg = expr

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		// Consume RPAREN
		p.consume()

		return lowerFunc, nil
	case "CAST":

		castFunc := &CastFunc{}

		p.consume() // Consume CAST

		// Look for LPAREN
		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		// Consume LPAREN

		p.consume() // Consume LPAREN

		// Parse value expression
		valueExpr, err := p.parseValueExpression()
		if err != nil {
			return nil, err
		}

		castFunc.Expr = valueExpr

		// Look for AS
		if p.peek(0).value != "AS" {
			return nil, errors.New("expected AS")

		}

		p.consume() // Consume AS

		if p.peek(0).tokenT != DATATYPE_TOK {
			return nil, errors.New("expected keyword")
		}

		castFunc.DataType = &Identifier{
			Value: p.peek(0).value.(string),
		}

		p.consume() // Consume datatype

		// Look for RPAREN
		if p.peek(0).tokenT != RPAREN_TOK {
			return nil, errors.New("expected )")
		}

		p.consume() // Consume RPAREN

		return castFunc, nil
	case "COALESCE":
		coalesceFunc := &CoalesceFunc{}

		p.consume() // Consume COALESCE

		if p.peek(0).tokenT != LPAREN_TOK {
			return nil, errors.New("expected (")
		}

		p.consume() // Consume LPAREN

		for p.peek(0).tokenT != RPAREN_TOK {
			// Parse value expression
			valueExpr, err := p.parseValueExpression()
			if err != nil {
				return nil, err
			}

			coalesceFunc.Args = append(coalesceFunc.Args, valueExpr)

			if p.peek(0).tokenT == COMMA_TOK {
				p.consume()
			}
		}

		p.consume() // Consume RPAREN

		// The last value expression is the default value
		coalesceFunc.Value = coalesceFunc.Args[len(coalesceFunc.Args)-1]

		// remove the last value expression from the args
		coalesceFunc.Args = coalesceFunc.Args[:len(coalesceFunc.Args)-1]

		return coalesceFunc, nil
	case "SYS_DATE":
		return &shared.SysDate{}, nil
	case "SYS_TIME":
		return &shared.SysTime{}, nil
	case "SYS_TIMESTAMP":
		return &shared.SysTimestamp{}, nil
	case "GENERATE_UUID":
		return &shared.GenUUID{}, nil
	default:
		return nil, errors.New("expected system function")

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
		} else if p.peek(0).tokenT == ASTERISK_TOK {
			aggFunc.Args = append(aggFunc.Args, &Wildcard{})

			p.consume()

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

// parseSubquery parses a subquery
func (p *Parser) parseSubquery() (*ValueExpression, error) {
	// Parse select statement
	selectStmt, err := p.parseSelectStmt()
	if err != nil {
		return nil, err
	}

	return &ValueExpression{
		Value: selectStmt,
	}, nil

}

// parseHavingClause parses a HAVING clause
func (p *Parser) parseHavingClause() (*HavingClause, error) {
	havingClause := &HavingClause{
		SearchCondition: make([]interface{}, 0),
	}

	var err error

	// Eat HAVING
	p.consume()

	// Parse search condition
	havingClause.SearchCondition, err = p.parseSearchCondition()
	if err != nil {
		return nil, err
	}

	return havingClause, nil
}
