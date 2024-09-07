// Package shared
// Shared functions between all packages
// Copyright (C) AriaSQL
// Author(s): Alex Gaetano Padula
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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// Shared between all packages

const VERSION = "ALPHA"

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
	PRIV_COMMIT
	PRIV_ROLLBACK
	PRIV_BEGIN
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

// CopyFile copies a file from src to dest
func CopyFile(srcPath, destPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	return err
}

// CopyDir copies a directory from src to dest
func CopyDir(srcDir, destDir string) error {
	// Create the destination directory
	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		return err
	}

	// Walk through the source directory
	return filepath.WalkDir(srcDir, func(srcPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Calculate the destination path
		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			return err
		}
		destPath := filepath.Join(destDir, relPath)

		if entry.IsDir() {
			// Create the directory
			err := os.MkdirAll(destPath, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			// Copy the file
			return CopyFile(srcPath, destPath)
		}

		return nil
	})
}

// GetColumns returns the columns of the data
func GetColumns(data []map[string]interface{}) []string {
	columns := make([]string, 0)

	if len(data) == 0 {
		return columns
	}

	for column := range data[0] {
		columns = append(columns, column)
	}

	sort.Sort(sort.StringSlice(columns))

	return columns
}

// DistinctMap returns a distinct map
func DistinctMap(data []map[string]interface{}, keys ...string) []map[string]interface{} {
	unique := make(map[string]bool)

	distinct := make([]map[string]interface{}, 0)
	for _, row := range data {
		key := ""
		for _, k := range keys {
			key += fmt.Sprintf("%v", row[k])
		}
		if _, ok := unique[key]; !ok {
			unique[key] = true
			distinct = append(distinct, row)
		}
	}

	return distinct
}
