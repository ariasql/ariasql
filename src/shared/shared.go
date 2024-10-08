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
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Shared between all packages

const VERSION = "ALPHA" // Version of AriaSQL

// DataTypes is a list of valid system data types
var DataTypes = []string{
	"CHAR", "CHARACTER", "DEC", "DECIMAL", "DOUBLE", "FLOAT", "SMALLINT", "INT", "INTEGER", "REAL", "NUMERIC",
	"DATE", "TIME", "TIMESTAMP", "DATETIME", "BINARY", "UUID", "BOOLEAN", "BOOL", "TEXT", "BLOB",
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
	PRIV_EXPLAIN
	PRIV_EXEC
	PRIV_DEALLOCATE
	PRIV_CLOSE
	PRIV_DECLARE
	PRIV_PRINT
	PRIV_FETCH
	PRIV_OPEN
	PRIV_WHILE
	PRIV_RETURN
	PRIV_BREAK
	PRIV_SET
	PRIV_EXIT
)

// SysDate represents system datetime/date function
type SysDate struct{} // Current DATE or DATETIME

// SysTime represents system time function
type SysTime struct{} // Current TIME

// SysTimestamp represents system timestamp function
type SysTimestamp struct{} // Current TIMESTAMP alias for DATETIME

// GenUUID represents generate UUID function
type GenUUID struct{} // Generate a UUID

// You grant privileges to a user on a database or table
// GRANT SELECT, INSERT, UPDATE, DELETE ON database.table TO user;

// PrivilegeActionToString converts a privilege action to a string
func (pa PrivilegeAction) String() string {
	return [...]string{"", "SELECT", "INSERT", "UPDATE", "DELETE", "ALTER", "DROP", "CREATE", "GRANT", "REVOKE", "SHOW", "CONNECT", "ALL", "COMMIT", "ROLLBACK", "BEGIN",
		"EXPLAIN", "EXEC", "DEALLOCATE", "CLOSE", "DECLARE", "PRINT", "FETCH", "OPEN", "WHILE", "RETURN", "BREAK", "SET", "EXIT"}[pa]

}

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
		if dt == strings.ToUpper(strings.TrimSpace(dataType)) {
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

// Get Headers Get the headers of the data
func GetHeaders(data []map[string]interface{}, sortColumns bool) []string {
	if len(data) == 0 {
		return []string{}
	}

	headers := make([]string, 0)
	for header := range data[0] {
		headers = append(headers, header)
	}

	if sortColumns {
		sort.Sort(sort.StringSlice(headers))

	}

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

// CreateJSONByteArray converts row data to JSON
func CreateJSONByteArray(data []map[string]interface{}) ([]byte, error) {
	marshal, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	return marshal, nil
}

// IsValidDateTimeFormat checks if the date time format is valid or not (format is YYYY-MM-DD HH:MM:SS)
func IsValidDateTimeFormat(dateTimeFormat string) bool {
	if len(dateTimeFormat) != 19 {
		return false
	}

	if dateTimeFormat[4] != '-' || dateTimeFormat[7] != '-' || dateTimeFormat[10] != ' ' || dateTimeFormat[13] != ':' || dateTimeFormat[16] != ':' {
		return false
	}

	return true
}

// IsValidTimeFormat checks if the time format is valid or not (format is HH:MM:SS)
func IsValidTimeFormat(timeFormat string) bool {
	if len(timeFormat) != 8 {
		return false
	}

	if timeFormat[2] != ':' || timeFormat[5] != ':' {
		return false
	}

	return true
}

// IsValidDateFormat checks if the date format is valid or not (format is YYYY-MM-DD)
func IsValidDateFormat(dateFormat string) bool {
	if len(dateFormat) != 10 {
		return false
	}

	if dateFormat[4] != '-' || dateFormat[7] != '-' {
		return false
	}

	return true
}

// StringToGOTime converts a string to a time.Time
func StringToGOTime(date string) (time.Time, error) {
	// DATE, TIME, TIMESTAMP, DATETIME
	toParse := []string{
		"2006-01-02",
		"15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, layout := range toParse {
		t, err := time.Parse(layout, date)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format")

}

// FormatToDate converts a time.Time to a string
func FormatToDate(date time.Time) string {
	return date.Format("2006-01-02")
}

// FormatToTime converts a time.Time to a string
func FormatToTime(date time.Time) string {
	return date.Format("15:04:05")
}

// FormatToDateTime converts a time.Time to a string
func FormatToDateTime(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}

// FormatToTimeStamp converts a time.Time to a string
func FormatToTimeStamp(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}

// GenerateUUID generates a UUID
func GenerateUUID() string {
	return uuid.New().String()
}

// ReverseString reverses a string
func ReverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// IdenticalMap checks if two maps are identical
func IdenticalMap(x, y map[string]interface{}) bool {
	if len(x) != len(y) {
		return false
	}

	for k, v := range x {
		if yv, ok := y[k]; !ok || !reflect.DeepEqual(v, yv) {
			return false
		}
	}

	return true
}

// RemoveSingleQuotesFromResult removes single quotes from strings in a result set
func RemoveSingleQuotesFromResult(data *[]map[string]interface{}) {
	for _, row := range *data {
		for key, value := range row {
			if _, ok := value.(string); ok {
				row[key] = strings.TrimPrefix(strings.TrimSuffix(value.(string), "'"), "'")
			}
		}
	}
}

// RemoveDupesStringSlice removes duplicates from a string slice
func RemoveDupesStringSlice(slice *[]string) []string {
	keys := make(map[string]bool)
	var list []string

	for _, entry := range *slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}

	return list
}
