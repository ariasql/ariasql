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
	"errors"
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
	Tables    map[string]*Table
	Directory string // Directory is the directory where database data is stored
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

// ColumnDefinition is a column definition
type ColumnDefinition struct {
	Name      string // Column name
	DataType  string // Column data type
	NotNull   bool   // Column cannot be null
	Sequence  bool   // Column is auto increment/sequence
	Unique    bool   // Column is unique
	Length    int    // Column length
	Scale     int    // Column scale
	Precision int    // Column precision
}

// Index is an index object
type Index struct {
	Name    string       // Name is the index name
	Columns []string     // Columns is a list of column names in the index
	Unique  bool         // Unique is true if the index is unique, there can only be one row with the same value
	btree   *btree.BTree // BTree is the Btree object for the index
}

// New creates a new catalog
func New(directory string) *Catalog {
	return &Catalog{
		Directory: directory,
	}
}

// Open initializes the catalog, reading all databases, tables, indexes, etc from disk
func (cat *Catalog) Open() error {
	cat.Databases = make(map[string]*Database)

	// Check for databases directory
	_, err := os.Stat(fmt.Sprintf("%s%sdatabases", cat.Directory, shared.GetOsPathSeparator()))
	if os.IsNotExist(err) {
		// Create databases directory
		err = os.MkdirAll(fmt.Sprintf("%s%sdatabases", cat.Directory, shared.GetOsPathSeparator()), 0755)
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
					Directory: fmt.Sprintf("%s%sdatabases%s%s", cat.Directory, shared.GetOsPathSeparator(), shared.GetOsPathSeparator(), databaseDir.Name()),
				}

				cat.Databases[databaseDir.Name()] = db

				// Within databases directory there are table directories

			}
		}

	}

	return nil
}

// Close closes the catalog
func (cat *Catalog) Close() {
	for _, db := range cat.Databases {
		for _, tbl := range db.Tables {
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

// CreateDatabase create a new database
func (cat *Catalog) CreateDatabase(name string) error {
	// Check if database exists
	if _, ok := cat.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	// Create database
	cat.Databases[name] = &Database{
		Tables:    make(map[string]*Table),
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

// DropTable drops a table by name
func (db *Database) DropTable(name string) error {
	// Check if table exists
	if _, ok := db.Tables[name]; !ok {
		return fmt.Errorf("table %s does not exist", name)
	}

	// Drop table
	delete(db.Tables, name)

	// Drop table directory
	err := os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
	if err != nil {
		return err
	}

	return nil

}

// CreateTable creates a new table in a schema
func (db *Database) CreateTable(name string, tblSchema *TableSchema) error {
	if tblSchema == nil {
		return fmt.Errorf("table schema is nil")
	}

	if len(name) > MAX_TABLE_NAME_SIZE {
		return fmt.Errorf("table name is too long, max length is %d", MAX_TABLE_NAME_SIZE)
	}

	// Check if table exists
	if _, ok := db.Tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}

	// Create table
	db.Tables[name] = &Table{
		Name:        name,
		Indexes:     make(map[string]*Index),
		TableSchema: tblSchema,
		Directory:   fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name),
	}

	// Create table directory
	err := os.Mkdir(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name), 0755)
	if err != nil {
		return err
	}

	sequenceDefined := false

	for colName, colDef := range tblSchema.ColumnDefinitions {
		if len(colName) > MAX_COLUMN_NAME_SIZE {
			// delete table
			delete(db.Tables, name)
			os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
			return fmt.Errorf("column name is too long, max length is %d", MAX_COLUMN_NAME_SIZE)
		}

		if !shared.IsValidDataType(colDef.DataType) {
			delete(db.Tables, name)
			os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
			return fmt.Errorf("invalid data type %s", colDef.DataType)
		}

		if colDef.Unique {
			err = db.Tables[name].CreateIndex(fmt.Sprintf("unique_%s", colName), []string{colName}, true)
			if err != nil {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return err
			}
		}

		if colDef.Sequence {
			if sequenceDefined {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("only one sequence column is allowed per table")
			}

			// Sequenced column must be unique and not null

			if !colDef.Unique || !colDef.NotNull {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("sequence column %s must be unique and not null", colName)
			}

			// Datatype MUST be an integer
			if strings.ToUpper(colDef.DataType) != "INT" && strings.ToUpper(colDef.DataType) != "INTEGER" {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("sequence column %s must be an integer", colName)
			}

			sequenceDefined = true
		}

		switch strings.ToUpper(colDef.DataType) {
		case "CHARACTER", "CHAR":
			// A character datatype requires a length
			if colDef.Length == 0 {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("column %s requires a length", colName)
			}
		case "NUMERIC", "DECIMAL", "DEC", "FLOAT", "DOUBLE", "REAL":
			// A numeric datatype requires a precision and scale
			if colDef.Precision == 0 {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("column %s requires a precision", colName)
			}

			if colDef.Scale == 0 {
				delete(db.Tables, name)
				os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
				return fmt.Errorf("column %s requires a scale", colName)
			}
		case "INT", "INTEGER", "SMALLINT":
			// An integer datatype does not require a precision or scale
		default:
			delete(db.Tables, name)
			os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
			return fmt.Errorf("invalid data type %s", colDef.DataType)
		}
	}

	// Create sequence file
	seqFile, err := os.Create(fmt.Sprintf("%s%s%s%s", db.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_SEQ_FILE_EXTENSION))
	if err != nil {
		delete(db.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	schemaFile, err := os.Create(fmt.Sprintf("%s%s%s%s", db.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_SCHEMA_FILE_EXTENSION))
	if err != nil {
		delete(db.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	defer schemaFile.Close()

	// Encode schema to file
	enc := gob.NewEncoder(schemaFile)

	err = enc.Encode(tblSchema)
	if err != nil {
		delete(db.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	// Create btree pager
	rowFile, err := btree.OpenPager(fmt.Sprintf("%s%s%s%s", db.Tables[name].Directory, shared.GetOsPathSeparator(), name, DB_SCHEMA_TABLE_DATA_FILE_EXTENSION), os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		delete(db.Tables, name)
		os.RemoveAll(fmt.Sprintf("%s%s%s", db.Directory, shared.GetOsPathSeparator(), name))
		return err
	}

	db.Tables[name].Rows = rowFile

	db.Tables[name].SequenceFile = seqFile
	db.Tables[name].SeqLock = &sync.Mutex{}

	return nil
}

// GetTable gets a table by name
func (db *Database) GetTable(tableName string) *Table {
	return db.Tables[tableName]
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

	bt, err := btree.Open(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), fmt.Sprintf("idx_%s", name), ".bt"), os.O_CREATE|os.O_RDWR, 0755, 6)
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
	indexFile, err := os.Create(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), fmt.Sprintf("idx_%s", name), DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION))
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
	err := os.Remove(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), fmt.Sprintf("idx_%s", name), DB_SCHEMA_TABLE_INDEX_FILE_EXTENSION))
	if err != nil {
		return err
	}

	// Remove btree file
	err = os.Remove(fmt.Sprintf("%s%s%s%s", tbl.Directory, shared.GetOsPathSeparator(), fmt.Sprintf("idx_%s", name), ".bt"))
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

		if colDef.NotNull && !colDef.Sequence {
			if _, ok := row[colName]; !ok {
				return fmt.Errorf("column %s cannot be null", colName)
			}
		}

		switch strings.ToUpper(colDef.DataType) {
		case "CHARACTER", "CHAR":
			if _, ok := row[colName].(string); !ok {

				// if column can be null, check if it is null
				if !colDef.NotNull {
					if row[colName] != nil {
						return fmt.Errorf("column %s is not a string", colName)
					}
				}

			} else {
				// Check length
				if len(row[colName].(string)) > colDef.Length {
					return fmt.Errorf("column %s is too long", colName)
				}
			}

		case "NUMERIC", "DECIMAL", "DEC", "FLOAT", "DOUBLE", "REAL":
			if _, ok := row[colName].(float64); !ok {
				return fmt.Errorf("column %s is not a float64", colName)
			}

			str := fmt.Sprintf("%.14g", row[colName].(float64))

			// Split the string on the decimal point
			parts := strings.Split(str, ".")

			if len(parts) > 1 {

				// The scale is the number of digits after the decimal point
				scale := len(parts[1])

				// The precision is the total number of digits
				precision := len(parts[0]) + len(parts[1])

				if colDef.Scale > 0 {
					// Check scale

					if scale > colDef.Scale {
						return fmt.Errorf("column %s has too many digits after the decimal point", colName)
					}

				}

				if colDef.Precision > 0 {
					// Check precision
					if precision > colDef.Precision {
						return fmt.Errorf("column %s is too large", colName)
					}
				}
			}

		case "INT", "INTEGER", "SMALLINT":
			// Check for sequence
			if colDef.Sequence {
				// Check if sequence column is unique
				idx := tbl.CheckIndexedColumn(colName, true)
				if idx == nil {
					return fmt.Errorf("sequence column %s must be unique", colName)
				}

				// Increment sequence
				seq, err := tbl.IncrementSequence()
				if err != nil {
					return err
				}

				row[colName] = seq
			}

			if _, ok := row[colName].(int); !ok {
				if _, ok := row[colName].(uint64); !ok {
					return fmt.Errorf("column %s is not an int", colName)
				} else {
					row[colName] = int(row[colName].(uint64))
				}

			}

			// Check if value fits in either INT/INTEGER, SMALLINT

			// Check if value fits in INT/INTEGER
			if strings.ToUpper(colDef.DataType) == "INT" || strings.ToUpper(colDef.DataType) == "INTEGER" {
				if row[colName].(int) > 2147483647 {
					return fmt.Errorf("column %s is too large for INT/INTEGER", colName)
				}
			}

			// Check if value fits in SMALLINT
			if strings.ToUpper(colDef.DataType) == "SMALLINT" {
				if row[colName].(int) > 32767 {
					return fmt.Errorf("column %s is too large for SMALLINT", colName)
				}
			}

		default:
			return fmt.Errorf("invalid data type %s", colDef.DataType)
		}

		if colDef.Unique {
			// Check if unique key exists
			if !colDef.Sequence {
				if _, ok := row[colName]; !ok {
					return fmt.Errorf("column %s cannot be null", colName)
				}
			}

			idx := tbl.CheckIndexedColumn(colName, true)
			if idx == nil {
				return fmt.Errorf("problem getting unique rows for column %s", colName)
			}

			// Check if unique key exists
			key, err := idx.btree.Get([]byte(fmt.Sprintf("%v", row[colName])))
			if err != nil {
				return fmt.Errorf("problem getting unique rows for column %s", colName)
			}

			if key != nil {

				for _, rowId := range key.V {
					// We store a []byte(rowId) in the btree
					// We need to convert it to an int64

					// Convert []byte to int64
					id, err := strconv.ParseInt(string(rowId), 10, 64)
					if err != nil {
						return errors.New("problem getting unique rows")
					}

					// Get row from table
					r, err := tbl.Rows.GetPage(id)
					if err != nil {
						return errors.New("problem getting unique rows")
					}

					// Decode row
					decoded, err := decodeRow(r)
					if err != nil {
						return errors.New("problem getting unique rows")
					}

					// Check if row exists
					if decoded[colName] == row[colName] {
						return fmt.Errorf("row with %s %v already exists", colName, row[colName])
					}

				}
			}

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

// GetBtree gets the btree for an index
func (idx *Index) GetBtree() *btree.BTree {
	return idx.btree
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

// Iterator is an iterator for rows in a table
type Iterator struct {
	table *Table
	row   int64
}

// GetRow gets a row by id
func (tbl *Table) GetRow(rowId int64) (map[string]interface{}, error) {
	// Read row from table
	row, err := tbl.Rows.GetPage(rowId)
	if err != nil {
		return nil, err
	}

	// decode row
	decoded, err := decodeRow(row)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

// NewIterator returns a new row iterator
func (tbl *Table) NewIterator() *Iterator {
	return &Iterator{
		table: tbl,
		row:   0,
	}
}

func (ri *Iterator) Current() int64 {
	return ri.row
}

// Next returns the next row in the table
func (ri *Iterator) Next() (map[string]interface{}, error) {
	for {
		if slices.Contains(ri.table.Rows.GetDeletedPages(), ri.row) {
			ri.row++
			continue

		} else {
			break
		}

	}

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

// Valid returns true if the iterator is valid
func (ri *Iterator) Valid() bool {
	return ri.row < ri.table.Rows.Count()

}

func (ri *Iterator) ValidUpdateIter() bool {
	return ri.row+1 < ri.table.Rows.Count()

}

// RowCount returns the number of rows in the table
func (tbl *Table) RowCount() int64 {
	return tbl.Rows.Count()
}

// CheckIndexedColumn checks if a column is indexed, if so return index
// If unique is true, check if the index is unique
func (tbl *Table) CheckIndexedColumn(column string, unique bool) *Index {
	for _, idx := range tbl.Indexes {
		if slices.Contains(idx.Columns, column) {

			if idx.Unique == unique {
				return idx
			}
		}
	}

	return nil
}

// GetUniqueIndex gets the first unique index for a table
func (tbl *Table) GetUniqueIndex() *Index {
	for _, idx := range tbl.Indexes {
		if idx.Unique {
			return idx
		}
	}

	return nil

}

// DeleteRow deletes a row from the table
func (tbl *Table) DeleteRow(rowId int64) error {
	// Read row from table
	row, err := tbl.Rows.GetPage(rowId)
	if err != nil {
		return err
	}

	// decode row
	decoded, err := decodeRow(row)
	if err != nil {
		return err
	}

	// Delete row from indexes
	for col, val := range decoded {
		for _, idx := range tbl.Indexes {
			if slices.Contains(idx.Columns, col) {
				// Remove from index
				err := idx.btree.Remove([]byte(fmt.Sprintf("%v", val)), []byte(fmt.Sprintf("%d", rowId)))
				if err != nil {
					return err
				}
			}
		}
	}

	// Delete row from table
	err = tbl.Rows.DeletePage(rowId)
	if err != nil {
		return err
	}

	return nil
}

// SetClause Set for update
type SetClause struct {
	ColumnName string
	Value      interface{}
}

// CopyRow copies a row
func CopyRow(row *map[string]interface{}) map[string]interface{} {
	newRow := make(map[string]interface{})
	for k, v := range *row {
		newRow[k] = v
	}
	return newRow
}

// UpdateRow updates a row in the table
func (tbl *Table) UpdateRow(rowId int64, row map[string]interface{}, sets []*SetClause) error {

	var prevRow map[string]interface{}

	for _, set := range sets {

		if _, ok := row[set.ColumnName]; !ok {
			return fmt.Errorf("column %s does not exist", set.ColumnName)
		}

		prevRow = CopyRow(&row)
		row[set.ColumnName] = set.Value

		// Check row against schema
		for colName, colDef := range tbl.TableSchema.ColumnDefinitions {
			if colName == set.ColumnName {
				switch strings.ToUpper(colDef.DataType) {
				case "CHARACTER", "CHAR":
					if _, ok := row[colName].(string); !ok {
						if !colDef.NotNull {
							if row[colName] != nil {
								return fmt.Errorf("column %s is not a string", colName)
							}
						}
					} else {
						// Check length
						if len(row[colName].(string)) > colDef.Length {
							return fmt.Errorf("column %s is too long", colName)
						}
					}

				case "NUMERIC", "DECIMAL", "DEC", "FLOAT", "DOUBLE", "REAL":
					if _, ok := row[colName].(float64); !ok {
						return fmt.Errorf("column %s is not a float64", colName)
					}

					str := fmt.Sprintf("%.14g", row[colName].(float64))

					// Split the string on the decimal point
					parts := strings.Split(str, ".")

					if len(parts) > 1 {

						// The scale is the number of digits after the decimal point
						scale := len(parts[1])

						// The precision is the total number of digits
						precision := len(parts[0]) + len(parts[1])

						if colDef.Scale > 0 {
							// Check scale

							if scale > colDef.Scale {
								return fmt.Errorf("column %s has too many digits after the decimal point", colName)
							}

						}

						if colDef.Precision > 0 {
							// Check precision
							if precision > colDef.Precision {
								return fmt.Errorf("column %s is too large", colName)
							}
						}
					}

				case "INT", "INTEGER", "SMALLINT":

					if _, ok := row[colName].(int); !ok {
						if _, ok := row[colName].(uint64); !ok {
							return fmt.Errorf("column %s is not an int", colName)
						} else {
							row[colName] = int(row[colName].(uint64))
						}
					}

					// Check if value fits in INT/INTEGER
					if strings.ToUpper(colDef.DataType) == "INT" || strings.ToUpper(colDef.DataType) == "INTEGER" {
						if row[colName].(int) > 2147483647 {
							return fmt.Errorf("column %s is too large for INT/INTEGER", colName)
						}
					}

					// Check if value fits in SMALLINT
					if strings.ToUpper(colDef.DataType) == "SMALLINT" {
						if row[colName].(int) > 32767 {
							return fmt.Errorf("column %s is too large for SMALLINT", colName)
						}
					}

				}

			}
		}

	}

	// Encode row
	encoded, err := encodeRow(row)
	if err != nil {
		return err
	}

	err = tbl.Rows.WriteTo(rowId, encoded)
	if err != nil {
		return err
	}

	for _, set := range sets {
		for colName, _ := range tbl.TableSchema.ColumnDefinitions {
			if colName == set.ColumnName {
				for _, idx := range tbl.Indexes {
					if slices.Contains(idx.Columns, colName) {
						// Remove old value from index
						err := idx.btree.Remove([]byte(fmt.Sprintf("%v", prevRow[colName])), []byte(fmt.Sprintf("%d", rowId)))
						if err != nil {
							return err
						}

						// Insert into index
						err = idx.btree.Put([]byte(fmt.Sprintf("%v", row[colName])), []byte(fmt.Sprintf("%d", rowId)))
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil

}
