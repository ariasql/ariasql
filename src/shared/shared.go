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

type ByColumn []map[string]interface{}

func (a ByColumn) Len() int {
	return len(a)
}

func (a ByColumn) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByColumn) Less(i, j int) bool {
	iName, _ := a[i]["name"].(string)
	jName, _ := a[j]["name"].(string)
	return iName < jName
}

func SortColumns(results []map[string]interface{}) {
	sort.Sort(ByColumn(results))
}
