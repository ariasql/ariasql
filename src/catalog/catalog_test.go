// Package catalog tests
// AriaSQL system catalog package tests
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
package catalog

import (
	"fmt"
	"os"
	"testing"
)

func TestNewCatalog(t *testing.T) {
	c := New("test/") // Catalog for databases, tables, etc
	if c == nil {
		t.Fatal("expected non-nil catalog")
	}
}

func TestCatalog_Open(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	// check if test directory exists

	stat, err := os.Stat("test/")
	if err != nil {
		return
	}

	if !stat.IsDir() {
		t.Fatal("expected test to be a directory")
	}

	// check if test/databases exists

	stat, err = os.Stat(fmt.Sprintf("test%sdatabases", string(os.PathSeparator)))
	if err != nil {
		t.Fatal(err)
	}

	if !stat.IsDir() {
		t.Fatal(fmt.Sprintf("expected test%sdatabases to be a directory", string(os.PathSeparator)))
	}

	defer c.Close()
}

func TestCatalog_Open2(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	// check if test directory exists

	stat, err := os.Stat("test/")
	if err != nil {
		return
	}

	if !stat.IsDir() {
		t.Fatal("expected test to be a directory")
	}

	// check if test/databases exists

	stat, err = os.Stat(fmt.Sprintf("test%sdatabases", string(os.PathSeparator)))
	if err != nil {
		t.Fatal(err)
	}

	if !stat.IsDir() {
		t.Fatal(fmt.Sprintf("expected test%sdatabases to be a directory", string(os.PathSeparator)))
	}

	// Create 5 databases
	for i := 0; i < 5; i++ {
		err = c.CreateDatabase(fmt.Sprintf("db%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// check if the databases were created
	for i := 0; i < 5; i++ {
		stat, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb%d", string(os.PathSeparator), string(os.PathSeparator), i))
		if err != nil {
			t.Fatal(err)
		}

		if !stat.IsDir() {
			t.Fatal(fmt.Sprintf("expected test%sdatabases%sdb%d to be a directory", string(os.PathSeparator), string(os.PathSeparator), i))
		}
	}

	c.Close()

	// Reopen the catalog
	err = c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	// check if the databases were reloaded into memory
	for i := 0; i < 5; i++ {
		db := c.GetDatabase(fmt.Sprintf("db%d", i))
		if db == nil {
			t.Fatal("expected non-nil database")
		}
	}

}

func TestCatalog_CreateDatabase(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	// check if the database was created
	stat, err := os.Stat(fmt.Sprintf("test%sdatabases%sdb1", string(os.PathSeparator), string(os.PathSeparator)))
	if err != nil {
		t.Fatal(err)
	}

	if !stat.IsDir() {
		t.Fatal(fmt.Sprintf("expected test%sdatabases%sdb1 to be a directory", string(os.PathSeparator), string(os.PathSeparator)))
	}

	// Check in-memory

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}
}

func TestCatalog_DropDatabase(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	err = c.DropDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	// check if the database was deleted
	_, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb1", string(os.PathSeparator), string(os.PathSeparator)))
	if err == nil {
		t.Fatal("expected db1 to be deleted")
	}
}

func TestDatabase_CreateTable(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Expect under table directory:
	// idx_unique_id.bt.del
	// table1.seq
	// table1.schma
	// table1.dat.del
	// table1.dat
	// idx_unique_name.idx
	// idx_unique_name.bt.del
	// idx_unique_name.bt
	// idx_unique_id.idx
	// idx_unique_id.bt

	expectedFiles := []string{
		"idx_unique_id.bt.del",
		"table1.seq",
		"table1.schma",
		"table1.dat.del",
		"table1.dat",
		"idx_unique_name.idx",
		"idx_unique_name.bt.del",
		"idx_unique_name.bt",
		"idx_unique_id.idx",
		"idx_unique_id.bt",
	}

	for _, file := range expectedFiles {
		_, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb1%stable1%s%s", string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator), file))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDatabase_DropTable(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.DropTable("table1")
	if err != nil {
		t.Fatal(err)
	}

	// check if the table was deleted
	_, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb1%stable1", string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator)))
	if err == nil {
		t.Fatal("expected table1 to be deleted")
	}
}

func TestCatalog_GetDatabase(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}
}

func TestDatabase_GetTable(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

}

func TestTable_CreateIndex(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   false,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	err = table.CreateIndex("name", []string{"name"}, true)
	if err != nil {
		t.Fatal(err)
	}

	// Expect under table directory:
	// idx_name.idx
	// idx_name.bt.del
	// idx_name.bt

	expectedFiles := []string{
		"idx_name.idx",
		"idx_name.bt.del",
		"idx_name.bt",
	}

	for _, file := range expectedFiles {
		_, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb1%stable1%s%s", string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator), file))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTable_GetIndex(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   false,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	err = table.CreateIndex("name", []string{"name"}, true)
	if err != nil {
		t.Fatal(err)
	}

	index := table.GetIndex("name")
	if index == nil {
		t.Fatal("expected non-nil index")
	}
}

func TestTable_DropIndex(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   false,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	err = table.CreateIndex("name", []string{"name"}, true)
	if err != nil {
		t.Fatal(err)
	}

	err = table.DropIndex("name")
	if err != nil {
		t.Fatal(err)
	}

	// check if the index was deleted
	_, err = os.Stat(fmt.Sprintf("test%sdatabases%sdb1%stable1%sidx_name.idx", string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator), string(os.PathSeparator)))
	if err == nil {
		t.Fatal("expected idx_name to be deleted")
	}
}

func TestTable_CheckIndexedColumn(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true, // should be indexed
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true, // should be indexed
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	idx := table.CheckIndexedColumn("name", true)

	if idx == nil {
		t.Fatal("expected non-nil index")
	}
}

func TestTable_IncrementSequence(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	seq, err := table.IncrementSequence()
	if err != nil {
		t.Fatal(err)
	}

	if seq != 1 {
		t.Fatalf("expected 1, got %d", seq)
	}

	seq, err = table.IncrementSequence()
	if err != nil {
		t.Fatal(err)
	}

	if seq != 2 {
		t.Fatalf("expected 2, got %d", seq)
	}
}

func TestTable_Insert(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	// Insert a row
	err = table.Insert([]map[string]interface{}{
		{
			"name": "John Doe",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check if the row was inserted
	row, err := table.GetRow(0)
	if err != nil {
		t.Fatal(err)
	}

	if row["name"] != "John Doe" {
		t.Fatalf("expected John Doe, got %s", row["name"])
	}

}

func TestTable_GetRow(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	// Insert a row
	err = table.Insert([]map[string]interface{}{
		{
			"name": "John Doe",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check if the row was inserted
	row, err := table.GetRow(0)
	if err != nil {
		t.Fatal(err)
	}

	if row["name"] != "John Doe" {
		t.Fatalf("expected John Doe, got %s", row["name"])
	}
}

func TestTable_RowCount(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	// Insert a row
	err = table.Insert([]map[string]interface{}{
		{
			"name": "John Doe",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check if the row count is 1
	count := table.RowCount()

	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

func TestTable_NewIterator(t *testing.T) {
	defer os.RemoveAll("test/")

	c := New("test/")
	err := c.Open()
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	err = c.CreateDatabase("db1")
	if err != nil {
		t.Fatal(err)
	}

	db := c.GetDatabase("db1")
	if db == nil {
		t.Fatal("expected non-nil database")
	}

	err = db.CreateTable("table1", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				DataType: "INT",
				NotNull:  true,
				Unique:   true,
				Sequence: true,
			},
			"name": {
				Name:     "name",
				DataType: "CHAR",
				Length:   50,
				NotNull:  true,
				Unique:   true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	table := db.GetTable("table1")
	if table == nil {
		t.Fatal("expected non-nil table")
	}

	// Insert a row
	err = table.Insert([]map[string]interface{}{
		{
			"name": "John Doe",
		},
		{
			"name": "Jane Doe",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	iter := table.NewIterator()

	for iter.Valid() {
		row, err := iter.Next()
		if err != nil {
			break
		}

		if row["name"] != "John Doe" && row["name"] != "Jane Doe" {
			t.Fatalf("expected John Doe or Jane Doe, got %s", row["name"])
		}
	}

}
