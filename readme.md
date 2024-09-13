<div>
    <h1 align="center"><img width="228" src="artwork/ariasql-logov1.png"></h1>
</div>

AriaSQL is a versatile relational database management system designed and engineered from the ground up from scratch to address a variety of data management needs with ease and efficiency.  At its core, AriaSQL is a database server that supports a range of SQL functionalities with a focus on delivering predictability, efficient query execution and data integrity.

> [!WARNING]
> Still in beta stages, use at your own risk.

## Features
- [x] SQL1+ handwritten parser, lexer implementation
- [x] BTrees for indexes
- [x] Optimized execution engine / compiler
- [x] SQL Server (TCP Server on port `3695`)
- [x] User authentication and privileges
- [x] Atomic transactions with rollback support on error
- [x] WAL (Write Ahead Logging)
- [x] Recovery-Replay from WAL
- [x] Subqueries
- [x] Aggregates
- [x] Implicit joins
- [x] Row level locking
- [x] Users and privileges
- [x] CLI (asql)
- [x] TLS Support
- [x] JSON response format (false by default)
- [x] Foreign keys
- [x] DML, DQL, DDL, DCL, TCL Support

## Whats coming?
- [ ] Views
- [ ] Triggers
- [ ] Stored Procedures
- [x] Cursors
- [ ] CTEs (Common Table Expressions)
- [ ] Window Functions
- [ ] Execution Plan using EXPLAIN - (Explains course of action for a query, shows order of operations, selected tables, and indexes to use if any or if a full table scan is required)
- [ ] Cascading options for CREATE TABLE i.e `ON DELETE CASCADE|SET NULL|SET DEFAULT|NO ACTION` `ON UPDATE CASCADE|SET NULL|SET DEFAULT|NO ACTION`
- [x] CASE expressions (Within select list and where clauses)
- [x] Functions (UPPER, LOWER, CAST, COALESCE, REVERSE, ROUND, POSITION, LENGTH, REPLACE, CONCAT, SUBSTRING, TRIM) `functions used with SELECT within a where clause or select list, i.e SELECT * FROM table WHERE UPPER(column) = 'TEST'`
- [x] DATE, TIME, TIMESTAMP, DATETIME, UUID, BINARY, BOOL/BOOLEAN, TEXT, BLOB data types
- [x] DEFAULT constraint
- [x] CHECK constraint
- [x] GENERATE_UUID, SYS_DATE, SYS_TIME, SYS_TIMESTAMP `functions which can be used with CREATE TABLE, or INSERT INTO, UPDATE, SELECT`
- [ ] Logging to file (aria.log)
- [ ] Roles
- [ ] Alter table (migration)

Above is expected for v1.0.0 release.

### v1.0.0+ - v2.0.0
- [ ] Replication - Replication to slave nodes, replicates data from master to slave nodes.
- [ ] Import/ Export (CSV, JSON, XML) - From (asql AriaSQL CLI or AriaSQL Developer)
- [ ] Encryption (ChaCha20) - After data is compressed it can be encrypted for storage
- [ ] Compression (ZSTD) - Compresses row data for storage

## Clients/Drivers
- GO - [github.com/ariasql/ariasql-go](https://github.com/ariasql/ariasql-go) `IN DEVELOPMENT`
- Python - [github.com/ariasql/ariasql-py](https://github.com/ariasql/ariasql-py)  `IN DEVELOPMENT`
- NodeJS - [github.com/ariasql/ariasql-node](https://github.com/ariasql/ariasql-node)  `IN DEVELOPMENT`
- Java - [github.com/ariasql/ariasql-java](https://github.com/ariasql/ariasql-java)  `IN DEVELOPMENT`
- Ruby - [github.com/ariasql/ariasql-ruby](https://github.com/ariasql/ariasql-ruby)  `IN DEVELOPMENT`
- PHP - [github.com/ariasql/ariasql-php](https://github.com/ariasql/ariasql-php)  `IN DEVELOPMENT`
- Rust - [github.com/ariasql/ariasql-rust](https://github.com/ariasql/ariasql-rust)  `IN DEVELOPMENT`
- C - [github.com/ariasql/ariasql-c](https://github.com/ariasql/ariasql-c)  `IN DEVELOPMENT`
- C# - [github.com/ariasql/ariasql-csharp](https://github.com/ariasql/ariasql-csharp)  `IN DEVELOPMENT`
- Objective-C - [github.com/ariasql/ariasql-objc](https://github.com/ariasql/ariasql-objc)  `IN DEVELOPMENT`
- C++ - [github.com/ariasql/ariasql-cpp](https://github.com/ariasql/ariasql-cpp)  `IN DEVELOPMENT`
- Swift - [github.com/ariasql/ariasql-swift](https://github.com/ariasql/ariasql-swift)  `IN DEVELOPMENT`
- Kotlin - [github.com/ariasql/ariasql-kotlin](https://github.com/ariasql/ariasql-kotlin)  `IN DEVELOPMENT`
- Scala - [github.com/ariasql/ariasql-scala](https://github.com/ariasql/ariasql-scala)  `IN DEVELOPMENT`
- Perl - [github.com/ariasql/ariasql-perl](https://github.com/ariasql/ariasql-perl)  `IN DEVELOPMENT`
- Lua - [github.com/ariasql/ariasql-lua](https://github.com/ariasql/ariasql-lua)  `IN DEVELOPMENT`
- R - [github.com/ariasql/ariasql-r](https://github.com/ariasql/ariasql-r)  `IN DEVELOPMENT`
- Julia - [github.com/ariasql/ariasql-julia](https://github.com/ariasql/ariasql-julia)  `IN DEVELOPMENT`
- Dart - [github.com/ariasql/ariasql-dart](https://github.com/ariasql/ariasql-dart)  `IN DEVELOPMENT`


## GUI
- AriaSQL Developer - [github.com/ariasql/developer]() `IN DEVELOPMENT`

## User Guide
The user guide will be released with the first stable release of AriaSQL for now it's best to reference executor tests or SQL1+ standard.

## Getting Started

The default user is `admin` with password `admin`.
This user has all privileges.

To update the password for the `admin` user, run the following SQL command:

```
ALTER USER admin SET PASSWORD 'newpassword';

-- To update a username
ALTER USER admin SET USERNAME 'newusername';
```

### The Server
The AriaSQL server starts when executing the binary.

```
./ariasql
```

When starting AriaSQL for the first time a variety of files will be created as seen below.
<div>
    <h1 align="center"><img width="760" src="artwork/asql2.png"></h1>
</div>

- ariaserver.yaml - The server configuration file.
- databases/ - The directory where databases and their data are stored.
- users.usrs - The file where users and their privileges are stored.
- wal.dat - The Write Ahead Log file.
- wal.dat.del - The Write Ahead Log file for deleted records. (Generated by underlaying pager)

#### Data directories
##### Windows

```
os.Getenv("ProgramData") + GetOsPathSeparator() + "AriaSQL"

i.e C:\ProgramData\AriaSQL
```

##### MacOS

```
/Library/Application Support/AriaSQL
```

##### Linux

```
/var/lib/ariasql
```


### Communicating with server
AriaSQL server uses a basic auth like mechanism to authenticate users.
The server listens on port `3695` for incoming connections.

You can configure your server settings in the `ariaserver.yaml` file.
<div>
    <h1 align="center"><img width="760" src="artwork/asql3.png"></h1>
</div>



You must encode the username and password in base64 similar to SMTP.

```
echo -n "admin\0admin" | base64

Above for example would be your base64 encoded auth string.

If you're using netcat simply pass the base64 encoded string as the first line.
```

#### Using asql - AriaSQL CLI
```
./asql -u admin -p admin -host localhost -port 3695 -tls false
```
All but username and password are optional.


<div>
    <h1 align="center"><img width="760" src="artwork/asql_rec2.gif"></h1>
</div>

### Setting server for JSON responses

You can execute `json on` or `json off` from client programs.


<div>
    <h1 align="center"><img width="460" src="artwork/jsonsql.png"></h1>
</div>


### SQL
AriaSQL Supports SQL1

#### Data Types
- INT
- INTEGER
- SMALLINT
- CHAR
- CHARACTER
- FLOAT
- DOUBLE
- DECIMAL
- DEC
- REAL
- NUMERIC
- DATE
- TIME
- TIMESTAMP
- DATETIME
- UUID
- BINARY
- BOOLEAN
- BOOL
- TEXT
- BLOB

#### Constraints
- UNIQUE
- NOT NULL
- DEFAULT
- CHECK
- PRIMARY KEY
- FOREIGN KEY
- REFERENCES

#### Aggregates
- COUNT `COUNT counts the number of rows based on arguments`
- SUM `SUM sums a column`
- AVG `AVG averages a column`
- MIN `MIN returns the minimum value`
- MAX `MAX returns the maximum value`

#### Functions
- UPPER `UPPER uppercases a string`
- LOWER `LOWER lowercases a string`
- CAST `CAST converts a string to a different data type`
- COALESCE `COALESCE replaces NULL with a value`
- REVERSE `REVERSE reverses a string`
- ROUND `ROUND rounds a number`
- POSITION `POSITION returns the position of a substring`
- LENGTH `LENGTH returns the length of a string`
- REPLACE `REPLACE replaces a substring`
- CONCAT `CONCAT concatenates strings`
- SUBSTRING `SUBSTRING returns a substring`
- TRIM `TRIM trims a string`

#### Create

##### Create Database
```
CREATE DATABASE test;
```

##### Create Table
```
CREATE TABLE test (id INT NOT NULL UNIQUE, name CHAR(255));
```

##### Create Index
```
CREATE INDEX test_id ON test (id);
OR
CREATE UNIQUE INDEX test_id ON test (id);
```

#### Show
```
SHOW DATABASES;
SHOW TABLES;
SHOW USERS;
```

#### Insert
```
INSERT INTO test (id, name) VALUES (1, 'test'), (2, 'test2');
```

#### Select
```
SELECT * FROM test;
```

#### Update
```
UPDATE test SET name = 'test3' WHERE id = 1;
```

#### Delete

```
DELETE FROM test WHERE id = 1;
```

#### Drop

```
DROP TABLE test;
DROP DATABASE test;
DROP INDEX test_id;
```

#### Grant

```
GRANT SELECT, INSERT, UPDATE, DELETE ON dbname.tablename TO user;
```

All

```
GRANT ALL ON *.* TO someusername;
```

#### Revoke

```
REVOKE SELECT, INSERT, UPDATE, DELETE ON dbname.tablename FROM user;
```

All

```
REVOKE ALL ON test FROM someusername;
```

#### Users

```
CREATE USER someusername WITH PASSWORD 'test';
```

#### Privileges

```
GRANT ALL ON dbname.* TO someusername;
```

#### Transactions
If a statement within a transaction fails, the transaction will be rolled back.

```
BEGIN;
INSERT INTO test (id, name) VALUES (1, 'test'), (2, 'test2');
COMMIT;
```

#### Rollback

```
BEGIN;
INSERT INTO test (id, name) VALUES (1, 'test'), (2, 'test2');
ROLLBACK;
```

#### Cursors
Declare a cursor variable.
```
DECLARE @PRODUCT_ID INT;
```

Declare a cursor.
```
DECLARE PRODUCT_CURSOR
CURSOR FOR SELECT ProductID FROM Products WHERE DiscontinuedDate IS NULL;
```

Open the cursor.
```
OPEN PRODUCT_CURSOR;
```

Fetch the cursor.
```
FETCH NEXT FROM PRODUCT_CURSOR INTO @PRODUCT_ID;
```

Iterate the cursor.
```
WHILE @@FETCH_STATUS = 0
BEGIN
PRINT @PRODUCT_ID;
    -- Do something with the product id

    FETCH NEXT FROM PRODUCT_CURSOR INTO @PRODUCT_ID;
END;
```

Close the cursor.
```
CLOSE PRODUCT_CURSOR;
```

Deallocate the cursor.
```
DEALLOCATE PRODUCT_CURSOR;
```

Deallocate the cursor variable.
```
DEALLOCATE @PRODUCT_ID;
```

For further examples, please see executor tests or ANSI SQL1 standard.

## Issues & Requests

Please report any issues or feature requests as an issue on this repository.

## License
AriaSQL is licensed under the AGPL-3.0 license.
