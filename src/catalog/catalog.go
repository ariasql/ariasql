// Package catalog
// AriaSQL system catalog package
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
	"ariasql/shared"
	"ariasql/storage/btree"
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const MAX_COLUMN_NAME_SIZE = 64 // Max 64 bytes for column name
const MAX_TABLE_NAME_SIZE = 64  // Max 64 bytes for table name
const MAX_INDEX_NAME_SIZE = 64  // Max 64 bytes for index name

// DB_SCHEMA_TABLE_SCHEMA_FILE_EXTENSION Table schema file extension
// The table schema file is used to store the schema of the table
const DB_SCHEMA_TABLE_SCHEMA_FILE_EXTENSION = ".schma" // Table schema file extension

// DB_SCHEMA_TABLE_DATA_FILE_EXTENSION Table data file extension
// The table data file is used to store the actual data of the table
const DB_SCHEMA_TABLE_DATA_FILE_EXTENSION = ".dat" // Table data

// DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION Index file extension
// The index file is used to store the index data
const DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION = ".idx" // Index file extension

// DB_SCHEMA_TABLE_SEQ_FILE_EXTENSION Table count file extension
// The table count file is used to store the number of rows in a table
// Used for sequence columns (there can only be one sequence column per table)
// The sequence column is a column that auto increments based on the number of rows in the table
const DB_SCHEMA_TABLE_SEQ_FILE_EXTENSION = ".seq" // Table seq file extension

// Catalog is the root of the database catalog
type Catalog struct {
	Databases map[string]*Database // Databases is a map of database names to database objects
	Directory string               // Directory is the directory where database catalog data is stored
}

// Database is a database object
type Database struct {
	Schemas   map[string]*Schema // Schemas is a map of schema names to schema objects
	Directory string             // Directory is the directory where database data is stored
}

// Schema is a schema object
type Schema struct {
	Tables    map[string]*Table // Tables is a map of table names to table objects
	Directory string            // Directory is the directory where schema data is stored
}

// Table is a table object
type Table struct {
	Name         string            // Name is the table name
	Indexes      map[string]*Index // Indexes is a map of index names to index objects
	Rows         *btree.Pager      // Rows is the btree pager for the table.  We use the pager to page our table data
	TableSchema  *TableSchema      // TableSchema is the schema of the table
	Directory    string            // Directory is the directory where table data is stored
	SequenceFile *os.File          // Table sequence file
	SeqLock      *sync.Mutex       // Sequence mutex
}

// TableSchema is the schema of a table
type TableSchema struct {
	ColumnDefinitions map[string]*ColumnDefinition // ColumnDefinitions is a map of column names to column definitions
}

// CascadeAction is an action to take on a cascade
// This can be set on table columns
type CascadeAction int

const (
	CascadeActionNone       CascadeAction = iota // CascadeActionNone
	CascadeActionSetNull                         // CascadeActionSetNull
	CascadeActionSetDefault                      // CascadeActionSetDefault
	CascadeActionCascade                         // CascadeActionCascade
	CascadeActionRestrict                        // CascadeActionRestrict
)

// ColumnDefinition is a column definition
type ColumnDefinition struct {
	Name          string        // Column name
	Datatype      string        // Column data type
	NotNull       bool          // Column cannot be null
	PrimaryKey    bool          // Column is a primary key
	Sequence      bool          // Column is auto increment/sequence
	Unique        bool          // Column is unique
	Length        int           // Column length
	Scale         int           // Column scale
	Precision     int           // Column precision
	Default       interface{}   // Column default value
	ForeignColumn string        // Foreign column name, if column is foreign key
	ForeignSchema *string       // Foreign schema name, if column is foreign key
	ForeignTable  *string       // Foreign table name, if column is foreign key
	IsForeign     bool          // Is foreign key
	OnDelete      CascadeAction // On delete action
	OnUpdate      CascadeAction // On update action
	// Check @todo
}

// Index is an index object
type Index struct {
	Name    string       // Name is the index name
	Columns []string     // Columns is a list of column names in the index
	Unique  bool         // Unique is true if the index is unique, there can only be one row with the same value
	btree   *btree.BTree // BTree is the Btree object for the index
}

// User is a database system user
type User struct {
	// @todo
}

func NewCatalog(directory string) *Catalog {
	return &Catalog{
		Directory: directory,
	}
}

// Initialize initializes the catalog, reading all databases, schemas, tables, indexes, etc from disk
func (cat *Catalog) Initialize() error {
	cat.Databases = make(map[string]*Database)

	// Check for databases directory
	_, err := os.Stat(fmt.Sprintf("%s%sdatabases", cat.Directory, shared.GetOsPathSeparator()))
	if os.IsNotExist(err) {
		// Create databases directory
		err = os.Mkdir(fmt.Sprintf("%s%sdatabases", cat.Directory, shared.GetOsPathSeparator()), 0755)
		if err != nil {
			return err
		}

	} else {
		// Read databases
		databaseDirs, err := os.ReadDir(fmt.Sprintf("%s%sdatabases", cat.Directory, shared.GetOsPathSeparator()))
		if err != nil {
			return err
		}

		for _, databaseDir := range databaseDirs {
			if databaseDir.IsDir() {
				db := &Database{
					Schemas:   make(map[string]*Schema),
					Directory: fmt.Sprintf("%s%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), shared.GetOsPathSeparator(), databaseDir.Name()),
				}

				cat.Databases[databaseDir.Name()] = db

				// Within databases directory there are schema directories
				schemaDirs, err := os.ReadDir(fmt.Sprintf("%s%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), shared.GetOsPathSeparator(), databaseDir.Name()))
				if err != nil {
					return err
				}

				for _, schemaDir := range schemaDirs {
					schema := &Schema{
						Tables: make(map[string]*Table),
					}

					db.Schemas[schemaDir.Name()] = schema

					// Within schema directories there are table directories
					tableDirs, err := os.ReadDir(fmt.Sprintf("%sdatabases%s%s%s%s", cat.Directory, shared.GetOsPathSeparator(), databaseDir.Name(), shared.GetOsPathSeparator(), schemaDir.Name()))
					if err != nil {
						return err
					}

					for _, tableDir := range tableDirs {
						if tableDir.IsDir() {
							table := &Table{
								Indexes: make(map[string]*Index),
							}

							schema.Tables[tableDir.Name()] = table

							table.Name = tableDir.Name()
							table.Directory = fmt.Sprintf("%sdatabases%s%s%s%s%s%s", cat.Directory, shared.GetOsPathSeparator(), databaseDir.Name(), shared.GetOsPathSeparator(), schemaDir.Name(), shared.GetOsPathSeparator(), tableDir.Name())

							// Read table schema
							schemaFile, err := os.Open(fmt.Sprintf("%s%s%s%s", table.Directory, shared.GetOsPathSeparator(), table.Name, DB_SCHEMA_TABLE_SCHEMA_FILE_EXTENSION))
							if err != nil {
								return err
							}

							defer schemaFile.Close()

							// Decode schema from file
							dec := gob.NewDecoder(schemaFile)

							err = dec.Decode(&table.TableSchema)

							if err != nil {
								return err
							}

							// Open btree pager
							rowFile, err := btree.OpenPager(fmt.Sprintf("%s%s%s%s", table.Directory, shared.GetOsPathSeparator(), table.Name, DB_SCHEMA_TABLE_DATA_FILE_EXTENSION), os.O_CREATE|os.O_RDWR, 0755)
							if err != nil {
								return err
							}

							table.Rows = rowFile

							// Read indexes
							indexFiles, err := os.ReadDir(fmt.Sprintf("%s%sdatabases%s%s%s%s%s%s", cat.Directory, shared.GetOsPathSeparator(), shared.GetOsPathSeparator(), databaseDir.Name(), shared.GetOsPathSeparator(), schemaDir.Name(), shared.GetOsPathSeparator(), tableDir.Name()))
							if err != nil {
								return err
							}

							for _, indexFile := range indexFiles {
								if strings.HasSuffix(indexFile.Name(), DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION) {

									indexFile, err := os.Open(fmt.Sprintf("%s%s%s", table.Directory, shared.GetOsPathSeparator(), indexFile.Name()))
									if err != nil {
										return err
									}

									defer indexFile.Close()

									// Decode index from file
									dec := gob.NewDecoder(indexFile)

									var index Index

									err = dec.Decode(&index)
									if err != nil {
										return err
									}

									// Open btree
									bt, err := btree.Open(fmt.Sprintf("%s%s%s%s", table.Directory, shared.GetOsPathSeparator(), index.Name, ".bt"), os.O_CREATE|os.O_RDWR, 0755, 6)
									if err != nil {
										return err
									}

									index.btree = bt

									table.Indexes[index.Name] = &index
								}
							}
						}
					}
				}
			}
		}

	}

	return nil
}

// Close closes the catalog
func (cat *Catalog) Close() {
	for _, db := range cat.Databases {
		for _, sch := range db.Schemas {
			for _, tbl := range sch.Tables {
				if tbl.Rows != nil {
					tbl.Rows.Close()
				}
				for _, idx := range tbl.Indexes {
					if idx.btree != nil {
						idx.btree.Close()
					}
				}
			}
		}
	}

}

// CreateDatabase create a new database
func (cat *Catalog) CreateDatabase(name string) error {
	// Check if database exists
	if _, ok := cat.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	// Create database
	cat.Databases[name] = &Database{
		Schemas:   make(map[string]*Schema),
		Directory: fmt.Sprintf("%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), name),
	}

	// Create database directory
	err := os.Mkdir(fmt.Sprintf("%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), name), 0755)
	if err != nil {
		return err
	}

	return nil
}

// DropDatabase drops a database by name
func (cat *Catalog) DropDatabase(name string) error {
	// Check if database exists
	if _, ok := cat.Databases[name]; !ok {
		return fmt.Errorf("database %s does not exist", name)
	}

	// Drop database
	delete(cat.Databases, name)

	// Drop database directory
	err := os.RemoveAll(fmt.Sprintf("%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), name))
	if err != nil {
		return err
	}

	return nil
}

// CreateSchema creates a new database schema
func (db *Database) CreateSchema(name string) error {
	// Check if schema exists
	if _, ok := db.Schemas[name]; ok {
		return fmt.Errorf("schema %s already exists", name)
	}

	// Create schema
	db.Schemas[name] = &Schema{
		Tables:    make(map[string]*Table),
		Directory: fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name),
	}

	// Create schema directory
	err := os.Mkdir(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name), 0755)
	if err != nil {
		return err
	}

	return nil
}

// DropSchema drops a schema by name and all of its tables
func (db *Database) DropSchema(name string) error {
	// Check if schema exists
	if _, ok := db.Schemas[name]; !ok {
		return fmt.Errorf("schema %s does not exist", name)
	}

	// Drop schema
	delete(db.Schemas, name)

	// Drop schema directory
	err := os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
	if err != nil {
		return err
	}

	return nil
}

// CreateTable creates a new table in a schema
func (sch *Schema) CreateTable(name string, tblSchema *TableSchema) error {
	if tblSchema == nil {
		return fmt.Errorf("table schema is nil")
	}

	if len(name) > MAX_TABLE_NAME_SIZE {
		return fmt.Errorf("table name is too long, max length is %d", MAX_TABLE_NAME_SIZE)
	}

	// Check if table exists
	if _, ok := sch.Tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}

	// Create table
	sch.Tables[name] = &Table{
		Name:        name,
		Indexes:     make(map[string]*Index),
		TableSchema: tblSchema,
		Directory:   fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name),
	}

	// Create table directory
	err := os.Mkdir(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name), 0755)
	if err != nil {
		return err
	}

	for colName, colDef := range tblSchema.ColumnDefinitions {
		if len(colName) > MAX_COLUMN_NAME_SIZE {
			// delete table
			delete(sch.Tables, name)
			os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
			return fmt.Errorf("column name is too long, max length is %d", MAX_COLUMN_NAME_SIZE)
		}

		if !shared.IsValidDataType(colDef.Datatype) {
			delete(sch.Tables, name)
			os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
			return fmt.Errorf("invalid data type %s", colDef.Datatype)
		}

		if colDef.PrimaryKey {
			// Create a new index
			err = sch.Tables[name].CreateIndex(fmt.Sprintf("primary_key_%s_%s", name, colName), []string{colName}, true)
			if err != nil {
				delete(sch.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
				return err
			}
		} else if colDef.Unique {
			err = sch.Tables[name].CreateIndex(fmt.Sprintf("unique_%s_%s", name, colName), []string{colName}, true)
			if err != nil {
				delete(sch.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
				return err
			}
		}
	}

	// Create sequence file
	seqFile, err := os.Create(fmt.Sprintf("%s%s%s%s", sch.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_SEQ_FILE_EXTENSION))
	if err != nil {
		delete(sch.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	schemaFile, err := os.Create(fmt.Sprintf("%s%s%s%s", sch.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_SCHEMA_FILE_EXTENSION))
	if err != nil {
		delete(sch.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	defer schemaFile.Close()

	// Encode schema to file
	enc := gob.NewEncoder(schemaFile)

	err = enc.Encode(tblSchema)
	if err != nil {
		delete(sch.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	// Create btree pager
	rowFile, err := btree.OpenPager(fmt.Sprintf("%s%s%s%s", sch.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_DATA_FILE_EXTENSION), os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		delete(sch.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	sch.Tables[name].Rows = rowFile

	sch.Tables[name].SequenceFile = seqFile
	sch.Tables[name].SeqLock = &sync.Mutex{}

	return nil
}

// DropTable drops a table by name
func (sch *Schema) DropTable(name string) error {
	// Check if table exists
	if _, ok := sch.Tables[name]; !ok {
		return fmt.Errorf("table %s does not exist", name)
	}

	// Drop table
	delete(sch.Tables, name)

	// Drop table directory
	err := os.RemoveAll(fmt.Sprintf("%s%s%s", sch.Directory, shared.GetOsPathSeparator(), name))
	if err != nil {
		return err
	}

	return nil
}

// CreateIndex creates a new index on a table
func (tbl *Table) CreateIndex(name string, columns []string, unique bool) error {
	if len(name) > MAX_INDEX_NAME_SIZE {
		return fmt.Errorf("index name is too long, max length is %d", MAX_INDEX_NAME_SIZE)
	}

	// Check if index exists
	if _, ok := tbl.Indexes[name]; ok {
		return fmt.Errorf("index %s already exists", name)
	}

	bt, err := btree.Open(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), name, ".bt"), os.O_CREATE|os.O_RDWR, 0755, 6)
	if err != nil {
		return err
	}

	// Create index
	tbl.Indexes[name] = &Index{
		Name:    name,
		Columns: columns,
		Unique:  unique,
		btree:   bt,
	}

	// Create index file
	indexFile, err := os.Create(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION))
	if err != nil {
		return err
	}

	defer indexFile.Close()

	// Encode index to file
	enc := gob.NewEncoder(indexFile)

	err = enc.Encode(tbl.Indexes[name])
	if err != nil {
		return err
	}

	return nil

}

// DropIndex drops an index by name
func (tbl *Table) DropIndex(name string) error {
	// Check if index exists
	if _, ok := tbl.Indexes[name]; !ok {
		return fmt.Errorf("index %s does not exist", name)
	}

	// Drop index
	delete(tbl.Indexes, name)

	// Drop index file
	err := os.Remove(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION))
	if err != nil {
		return err
	}

	// Remove btree file
	err = os.Remove(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), name, ".bt"))
	if err != nil {
		return err
	}

	return nil
}

// GetDatabase gets a database by name
func (cat *Catalog) GetDatabase(name string) *Database {

	return cat.Databases[name]

	return nil
}

// GetSchema gets a schema by name
func (db *Database) GetSchema(name string) *Schema {
	return db.Schemas[name]
}

// GetTable gets a table by name
func (sch *Schema) GetTable(name string) *Table {
	return sch.Tables[name]
}

// GetIndex gets an index by name
func (tbl *Table) GetIndex(name string) *Index {
	return tbl.Indexes[name]
}

// Insert inserts a row into the table
func (tbl *Table) Insert(rows []map[string]interface{}) error {
	for _, row := range rows {
		// Insert row into table
		err := tbl.insert(row)
		if err != nil {
			return err
		}
	}

	return nil
}

// insert inserts a row into the table
func (tbl *Table) insert(row map[string]interface{}) error {
	// Check row against schema
	for colName, colDef := range tbl.TableSchema.ColumnDefinitions {
		if colDef.NotNull {
			if _, ok := row[colName]; !ok {
				return fmt.Errorf("column %s cannot be null", colName)
			}
		}

		if colDef.PrimaryKey && !colDef.Sequence {
			// Check if primary key exists
			if _, ok := row[colName]; !ok {
				return fmt.Errorf("column %s cannot be null", colName)
			}
		} else if colDef.PrimaryKey && colDef.Sequence {
			// Increment sequence
			seq, err := tbl.IncrementSequence()
			if err != nil {
				return err
			}

			row[colName] = seq
		}

		if colDef.Unique {
			// Check if unique key exists
			if _, ok := row[colName]; !ok {
				return fmt.Errorf("column %s cannot be null", colName)
			}

		}

		if colDef.IsForeign {
			// Check if foreign key exists
			if _, ok := row[colName]; !ok {
				return fmt.Errorf("column %s cannot be null", colName)
			}
		}

		switch strings.ToUpper(colDef.Datatype) {
		case "CHARACTER", "CHAR", "TEXT":
			if _, ok := row[colName].(string); !ok {
				return fmt.Errorf("column %s is not a string", colName)
			}
		case "NUMERIC", "DECIMAL", "DEC":
			if _, ok := row[colName].(float64); !ok {
				return fmt.Errorf("column %s is not a float64", colName)
			}

			if colDef.Scale > 0 {
				// Check scale

			}

			if colDef.Precision > 0 {
				// Check precision
			}

		case "INT", "INTEGER", "SMALLINT", "BIGINT":
			if _, ok := row[colName].(int); !ok {
				return fmt.Errorf("column %s is not an int", colName)
			}

		case "DATE", "DATETIME", "TIME", "TIMESTAMP":
			if _, ok := row[colName].(string); !ok {
				return fmt.Errorf("column %s is not a string", colName)
			}

		case "BOOLEAN", "BOOL":
			if _, ok := row[colName].(bool); !ok {
				return fmt.Errorf("column %s is not a bool", colName)
			}

		case "UUID":
			if _, ok := row[colName].(string); !ok {
				return fmt.Errorf("column %s is not a string", colName)
			}

		case "BINARY":
			if _, ok := row[colName].(string); !ok {
				return fmt.Errorf("column %s is not a string", colName)
			}

		default:
			return fmt.Errorf("invalid data type %s", colDef.Datatype)
		}

	}

	// Write row to table
	rowId, err := tbl.writeRow(row)
	if err != nil {
		return err
	}

	// Insert row into indexes
	for col, val := range row {
		for _, idx := range tbl.Indexes {
			if slices.Contains(idx.Columns, col) {
				// Insert into index
				err := idx.btree.Put([]byte(fmt.Sprintf("%v", val)), []byte(fmt.Sprintf("%d", rowId)))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// writeRow writes a row to the table
func (tbl *Table) writeRow(row map[string]interface{}) (int64, error) {
	// Write row to table

	// encode row to bytes
	encoded, err := encodeRow(row)
	if err != nil {
		return -1, err
	}

	rowId, err := tbl.Rows.Write(encoded)
	if err != nil {
		return -1, err
	}

	return rowId, nil
}

// encodeRow encodes a row to a byte slice
func encodeRow(n map[string]interface{}) ([]byte, error) {
	// use gob
	buff := new(bytes.Buffer)

	enc := gob.NewEncoder(buff)
	err := enc.Encode(n)

	if err != nil {
		return nil, err

	}

	return buff.Bytes(), nil
}

// decodeRow decodes a row from a byte slice
func decodeRow(b []byte) (map[string]interface{}, error) {
	var decoded map[string]interface{}

	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&decoded)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

// IncrementSequence increments the sequence for the table
func (tbl *Table) IncrementSequence() (int, error) {
	tbl.SeqLock.Lock()
	defer tbl.SeqLock.Unlock()
	d, err := os.ReadFile(tbl.SequenceFile.Name())

	if string(d) == "" {
		tbl.SequenceFile.Write([]byte("1"))
		return 1, nil
	}

	i, err := strconv.Atoi(string(d))
	if err != nil {
		return 0, err
	}

	j := i + 1
	tbl.SequenceFile.Truncate(0)
	tbl.SequenceFile.Seek(0, os.SEEK_SET)
	tbl.SequenceFile.Write([]byte(fmt.Sprintf("%d", j)))

	return j, nil

	return 0, nil
}

// RowIterator is an iterator for rows in a table
type RowIterator struct {
	table *Table
	row   int64
}

// RowsIterator returns a new row iterator
func (tbl *Table) RowsIterator() *RowIterator {
	return &RowIterator{
		table: tbl,
		row:   0,
	}
}

// Next returns the next row in the table
func (ri *RowIterator) Next() (map[string]interface{}, error) {
	// Read row from table
	row, err := ri.table.Rows.GetPage(ri.row)
	if err != nil {
		return nil, err
	}

	// decode row
	decoded, err := decodeRow(row)
	if err != nil {
		ri.row++
		// When decoding next a row can be an overflow or deleted that is why we skip it
		return nil, nil
	}

	ri.row++

	return decoded, nil
}
