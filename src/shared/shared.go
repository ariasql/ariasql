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
	"os"
	"runtime"
	"strings"
)

// Shared between all packages

// dataTypes is a list of valid system data types
var dataTypes = []string{
	"CHARACTER", "CHAR", "TEXT", "NUMERIC", "DECIMAL", "DEC",
	"INT", "INTEGER", "SMALLINT", "BIGINT", "DATE", "DATETIME", "TIME", "TIMESTAMP", "BOOLEAN", "BOOL", "UUID", "BINARY",
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
	for _, dt := range dataTypes {
		if dt == strings.ToUpper(dataType) {
			return true
		}
	}
	return false
}
