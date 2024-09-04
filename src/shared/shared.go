// Package shared
// Shared functions between all packages
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
package shared

import (
	"bytes"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"os"
	"runtime"
	"sort"
	"strings"
)

// Shared between all packages

// DataTypes is a list of valid system data types
var DataTypes = []string{
	"CHAR", "CHARACTER", "DEC", "DECIMAL", "DOUBLE", "FLOAT", "SMALLINT", "INT", "INTEGER", "REAL", "NUMERIC",
}

// PrivilegeAction represents a privilege action
type PrivilegeAction int

const (
	_ PrivilegeAction = iota
	PRIV_SELECT
	PRIV_INSERT
	PRIV_UPDATE
	PRIV_DELETE
	PRIV_ALTER
	PRIV_DROP
	PRIV_CREATE
	PRIV_GRANT
	PRIV_REVOKE
	PRIV_SHOW
	PRIV_CONNECT // Connect to aria server
	PRIV_ALL
)

// You grant privileges to a user on a database or table
// GRANT SELECT, INSERT, UPDATE, DELETE ON database.table TO user;

// GetDefaultDataDir returns the default data directory for the current OS
func GetDefaultDataDir() string {
	// A user of AriaSQL can set the data directory, if not set we use the default which would be preferred
	switch runtime.GOOS {
	case "windows":
		return os.Getenv("ProgramData") + GetOsPathSeparator() + "AriaSQL"
	case "darwin":
		return "/Library/Application Support/AriaSQL"
	default:
		return "/var/lib/ariasql"
	}
}

// GetOsPathSeparator get correct path separator for the OS
func GetOsPathSeparator() string {
	if os.PathSeparator == '\\' {
		return "\\"
	}
	return "/"
}

// IsValidDataType checks if the data type is valid
func IsValidDataType(dataType string) bool {
	for _, dt := range DataTypes {
		if dt == strings.ToUpper(dataType) {
			return true
		}
	}
	return false
}

// getColumnWidths Get the maximum width of each column
func getColumnWidths(data []map[string]interface{}, headers []string) map[string]int {
	widths := make(map[string]int)
	for _, header := range headers {
		widths[header] = len(header)
	}
	for _, row := range data {
		for _, header := range headers {
			value := fmt.Sprintf("%v", row[header])
			if len(value) > widths[header] {
				widths[header] = len(value)
			}
		}
	}
	return widths
}

func GetHeaders(data []map[string]interface{}) []string {
	if len(data) == 0 {
		return []string{}
	}
	headers := make([]string, 0)
	for header := range data[0] {
		headers = append(headers, header)
	}

	sort.Sort(sort.StringSlice(headers))

	return headers
}

// CreateTableByteArray Create the table as a byte array
func CreateTableByteArray(data []map[string]interface{}, headers []string) []byte {
	if len(data) == 0 {
		return []byte{}
	}

	var buffer bytes.Buffer
	widths := getColumnWidths(data, headers)

	// Create the border
	border := "+"
	for _, header := range headers {
		border += strings.Repeat("-", widths[header]+2) + "+"
	}
	buffer.WriteString(border + "\n")

	// Print headers
	headerLine := "|"
	for _, header := range headers {
		headerLine += " " + fmt.Sprintf("%-*v", widths[header], header) + " |"
	}
	buffer.WriteString(headerLine + "\n")
	buffer.WriteString(border + "\n")

	// Print rows
	for _, row := range data {
		rowLine := "|"
		for _, header := range headers {
			value := fmt.Sprintf("%v", row[header])
			rowLine += " " + fmt.Sprintf("%-*v", widths[header], value) + " |"
		}
		buffer.WriteString(rowLine + "\n")
	}
	buffer.WriteString(border + "\n")

	return buffer.Bytes()
}

// HashPassword hashes the password using bcrypt
func HashPassword(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}

	return string(hashedPassword), nil

}

// ComparePasswords compares the hashed password with the password
func ComparePasswords(hashedPassword, password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		return false
	}
	return true
}
