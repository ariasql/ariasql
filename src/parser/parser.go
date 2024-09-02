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
	"ariasql/shared"
	"errors"
	"log"
	"strconv"
	"strings"
)

var (
	keywords = append([]string{
		"ALL", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "AVG",
		"BEGIN", "BETWEEN", "BY", "CHECK", "CLOSE", "COBOL", "COMMIT",
		"CONTINUE", "COUNT", "CREATE", "CURRENT", "CURSOR", "DECLARE", "DELETE", "DESC", "DISTINCT",
		"END", "ESCAPE", "EXEC", "EXISTS",
		"FETCH", "FOR", "FORTRAN", "FOUND", "FROM",
		"GO", "GOTO", "GRANT", "GROUP", "HAVING",
		"IN", "INDICATOR", "INSERT", "INTO", "IS", "SEQUENCE",
		"LANGUAGE", "LIKE",
		"MAX", "MIN", "MODULE", "NOT", "NULL",
		"OF", "ON", "OPEN", "OPTION", "OR", "ORDER",
		"PASCAL", "PLI", "PRECISION", "PRIVILEGES", "PROCEDURE", "PUBLIC", "ROLLBACK",
		"SCHEMA", "SECTION", "SELECT", "SET", "SOME",
		"SQL", "SQLCODE", "SQLERROR", "SUM",
		"TABLE", "TO", "UNION", "UNIQUE", "UPDATE", "USER",
		"VALUES", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK",
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
		case "SELECT":
			return p.parseSelectStmt()
		}
	}

	return nil, errors.New("expected keyword")

}

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

func (p *Parser) parseComparisonExpr() (*ComparisonPredicate, error) {
	// Parse left side of comparison
	left, err := p.parseValueExpression()
	if err != nil {
		return nil, err
	}

	log.Println("WTF")

	log.Println(p.peek(0).value)

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

func (p *Parser) parseLiteral() (interface{}, error) {
	if p.peek(0).tokenT != LITERAL_TOK {
		return nil, errors.New("expected literal")
	}

	lit := p.peek(0).value

	p.consume()

	return &Literal{Value: lit}, nil
}
