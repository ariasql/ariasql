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
	"os"
	"testing"
	"time"
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

func TestIsValidDateFormat(t *testing.T) {
	date := "2006-01-02"

	if !IsValidDateFormat(date) {
		t.Errorf("expected %v to be valid", date)
	}

}

func TestReverseString(t *testing.T) {
	str := "hello"
	reversed := ReverseString(str)
	if reversed != "olleh" {
		t.Errorf("expected olleh, got %s", reversed)
	}
}

func TestIdenticalMaps(t *testing.T) {
	m1 := map[string]interface{}{
		"ID":   1,
		"Name": "John",
	}

	m2 := map[string]interface{}{
		"ID":   1,
		"Name": "John",
	}

	if !IdenticalMap(m1, m2) {
		t.Errorf("expected maps to be identical")
	}
}

func TestFormatToTimeStamp(t *testing.T) {
	ti := time.Now()
	timestamp := FormatToTimeStamp(ti)

	if timestamp != ti.Format("2006-01-02 15:04:05") {
		t.Errorf("expected %v, got %v", ti.Format("2006-01-02 15:04:05"), timestamp)
	}
}

func TestFormatToDate(t *testing.T) {
	ti := time.Now()
	date := FormatToDate(ti)

	if date != ti.Format("2006-01-02") {
		t.Errorf("expected %v, got %v", ti.Format("2006-01-02"), date)
	}
}

func TestFormatToDateTime(t *testing.T) {
	ti := time.Now()
	datetime := FormatToDateTime(ti)

	if datetime != ti.Format("2006-01-02 15:04:05") {
		t.Errorf("expected %v, got %v", ti.Format("2006-01-02 15:04:05"), datetime)
	}
}

func TestFormatToTime(t *testing.T) {
	ti := time.Now()
	time := FormatToTime(ti)

	if time != ti.Format("15:04:05") {
		t.Errorf("expected %v, got %v", ti.Format("15:04:05"), time)
	}
}

func TestStringToGOTime(t *testing.T) {
	toParse := []string{
		"2006-01-02",
		"15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, s := range toParse {
		_, err := StringToGOTime(s)
		if err != nil {
			t.Errorf("expected %v to be valid", s)
		}
	}
}

func TestIsValidTimeFormat(t *testing.T) {
	valid := []string{
		"15:04:05",
	}

	for _, v := range valid {
		if !IsValidTimeFormat(v) {
			t.Errorf("expected %v to be valid", v)
		}
	}
}

func TestIsValidDateTimeFormat(t *testing.T) {
	valid := []string{
		"2006-01-02 15:04:05",
	}

	for _, v := range valid {
		if !IsValidDateTimeFormat(v) {
			t.Errorf("expected %v to be valid", v)
		}
	}
}

func TestCopyDir(t *testing.T) {
	src := "testdata"
	dest := "testdata_copy"

	defer os.RemoveAll(src)
	defer os.RemoveAll(dest)

	os.Mkdir(src, 0755)

	err := CopyDir(src, dest)
	if err != nil {
		t.Error(err)
	}

	// Check if the directory was copied
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		t.Error("expected directory to be copied")
	}

}
