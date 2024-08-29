// Package catalog
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
	"os"
	"testing"
)

func TestCatalog_Initialize(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	defer cat.Close()

	// Check for databases directory
	_, err = os.Stat("databases")
	if err != nil {
		t.Fatal(err)
		return

	}

}

func TestCatalog_CreateDatabase(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	if cat.GetDatabase("test") == nil {
		t.Fatal("database not found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test")
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestCatalog_DropDatabase(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.DropDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	if cat.GetDatabase("test") != nil {
		t.Fatal("database found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test")
	if err == nil {
		t.Fatal("database directory found")
		return
	}
}

func TestCatalog_CreateSchema(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	if db.GetSchema("test") == nil {
		t.Fatal("schema not found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test/test")
	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestCatalog_DropSchema(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	err = db.DropSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	if db.GetSchema("test") != nil {
		t.Fatal("schema found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test/test")
	if err == nil {
		t.Fatal("schema directory found")
		return
	}
}

func Test_Catalog_CreateTable(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	if schema.GetTable("test") == nil {
		t.Fatal("table not found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test/test/test")
	if err != nil {
		t.Fatal(err)
		return
	}
}

func Test_Catalog_DropTable(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	if schema.GetTable("test") == nil {
		t.Fatal("table not found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test/test/test")
	if err != nil {
		t.Fatal(err)
		return
	}

	err = schema.DropTable("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	if schema.GetTable("test") != nil {
		t.Fatal("table found")
		return
	}

	// Check for databases directory
	_, err = os.Stat("databases/test/test/test")
	if err == nil {
		t.Fatal("table directory found")
		return
	}
}

func TestCatalog_CreateIndex(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	tbl := schema.GetTable("test")

	if tbl == nil {
		t.Fatal("table not found")
		return
	}

	err = tbl.CreateIndex("id_idx", []string{"id"}, false)
	if err != nil {
		t.Fatal(err)
		return
	}

	if tbl.GetIndex("id_idx") == nil {
		t.Fatal("index not found")
		return
	}

	// Check for id_idx.bt
	_, err = os.Stat("databases/test/test/test/id_idx.bt")
	if err != nil {
		t.Fatal(err)
		return
	}

}

func TestCatalog_DropIndex(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	tbl := schema.GetTable("test")

	if tbl == nil {
		t.Fatal("table not found")
		return
	}

	err = tbl.CreateIndex("id_idx", []string{"id"}, false)
	if err != nil {
		t.Fatal(err)
		return
	}

	if tbl.GetIndex("id_idx") == nil {
		t.Fatal("index not found")
		return
	}

	// Check for id_idx.bt
	_, err = os.Stat("databases/test/test/test/id_idx.bt")
	if err != nil {
		t.Fatal(err)
		return
	}

	err = tbl.DropIndex("id_idx")
	if err != nil {
		t.Fatal(err)
		return
	}

	if tbl.GetIndex("id_idx") != nil {
		t.Fatal("index found")
		return
	}

	// Check for id_idx.bt
	_, err = os.Stat("databases/test/test/test/id_idx.bt")
	if err == nil {
		t.Fatal("index directory found")
		return
	}
}

func TestIncrementSequence(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	tbl := schema.GetTable("test")

	if tbl == nil {
		t.Fatal("table not found")
		return
	}

	for i := 0; i < 50; i++ {
		if i == 0 {
			continue
		}
		val, err := tbl.IncrementSequence()
		if err != nil {
			t.Fatal(err)
			return
		}

		if val != i {
			t.Fatalf("expected %d, got %d", i, val)
			return
		}
	}

}

func TestCatalog_Insert(t *testing.T) {
	defer os.RemoveAll("databases")
	cat := NewCatalog("./")
	err := cat.Initialize()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = cat.CreateDatabase("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	db := cat.GetDatabase("test")
	err = db.CreateSchema("test")
	if err != nil {
		t.Fatal(err)
		return
	}

	schema := db.GetSchema("test")
	err = schema.CreateTable("test", &TableSchema{
		ColumnDefinitions: map[string]*ColumnDefinition{
			"id": {
				Name:     "id",
				Datatype: "int",
			},
			"name": {
				Name:     "name",
				Datatype: "text",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
		return
	}

	tbl := schema.GetTable("test")

	if tbl == nil {
		t.Fatal("table not found")
		return
	}

	for i := 0; i < 50; i++ {
		err = tbl.Insert([]map[string]interface{}{{
			"id":   i,
			"name": "test",
		}})
		if err != nil {
			t.Fatal(err)
			return
		}
	}

	iter := tbl.RowsIterator()

	for i := 0; i < 50; i++ {
		row, err := iter.Next()
		if err != nil {
			t.Fatal(err)
			return
		}

		if row["id"] != i {
			t.Fatalf("expected %d, got %d", i, row["id"])
			return
		}
	}

}
