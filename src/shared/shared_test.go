// Package shared tests
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
	"log"
	"testing"
)

func TestIsValidDataType(t *testing.T) {
	valid := DataTypes

	for _, v := range valid {
		if !IsValidDataType(v) {
			t.Errorf("expected %v to be valid", v)
		}
	}
}

func TestGetHeaders(t *testing.T) {
	data := []map[string]interface{}{
		{
			"ID":   1,
			"Name": "John",
		},
	}

	headers := GetHeaders(data)
	if len(headers) != 2 {
		t.Errorf("expected 2 headers, got %d", len(headers))
	}

}

func TestGetColumnWidths(t *testing.T) {
	data := []map[string]interface{}{
		{
			"ID":   1,
			"Name": "John",
		},
	}

	headers := GetHeaders(data)
	widths := getColumnWidths(data, headers)
	if widths["ID"] != 2 {
		t.Errorf("expected width of 2, got %d", widths["ID"])
	}

	if widths["Name"] != 4 {
		t.Errorf("expected width of 4, got %d", widths["Name"])
	}
}

func TestCreateTableByteArray(t *testing.T) {
	data := []map[string]interface{}{
		{
			"ID":   1,
			"Name": "John",
		},
	}

	headers := GetHeaders(data)
	b := CreateTableByteArray(data, headers)
	if len(b) == 0 {
		t.Errorf("expected non-empty byte array")
	}

	expect := `+----+------+
| ID | Name |
+----+------+
| 1  | John |
+----+------+
`

	if string(b) != expect {
		log.Println(string(b))
		t.Errorf("expected %s, got %s", expect, string(b))
	}
}

func TestDistinctMap(t *testing.T) {
	data := []map[string]interface{}{
		{
			"ID":   1,
			"Name": "John",
		},
		{
			"ID":   1,
			"Name": "John",
		},
		{
			"ID":   2,
			"Name": "John",
		},
	}

	distinct := DistinctMap(data, GetColumns(data)...)
	if len(distinct) != 2 {
		t.Errorf("expected 2 rows, got %d", len(distinct))
	}

}
