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
