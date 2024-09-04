<div>
    <h1 align="center"><img width="228" src="artwork/ariasql-logov1.png"></h1>
</div>

AriaSQL is an open source relational database server implementing SQL.
AriaSQL is still in the beginning stages of development and is not ready for production use.


## Features
- [x] SQL1 handwritten parser, lexer implementation
- [x] BTrees for indexes
- [x] Execution engine / Compiler
- [x] SQL Server (TCP Server on port `3695`)
- [x] User authentication and privileges
- [x] Transactions with rollbacks
- [x] WAL (Write Ahead Logging)
- [x] Recovery
- [x] Subqueries
- [x] Row level locking
- [x] Users and privileges

## Getting Started

The default user is `admin` with password `admin`.
This user has all privileges.

To update the password for the `admin` user, run the following SQL command:

```
ALTER USER admin SET PASSWORD 'newpassword';

-- To update a username
ALTER USER admin SET USERNAME 'newusername';
```



### Communicating with server
AriaSQL server uses a basic auth like mechanism to authenticate users.
The server listens on port `3695` for incoming connections.

You must encode the username and password in base64 similar to SMTP.

```
echo -n "admin\0admin" | base64

Above for example would be your base64 encoded auth string.

If you're using netcat simply pass the base64 encoded string as the first line.
```

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

#### Constraints
- UNIQUE
- NOT NULL

#### Aggregates
- COUNT
- SUM
- AVG
- MIN
- MAX

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

For further examples, please see executor tests or ANSI SQL1 standard.

## Issues & Requests

Please report any issues or feature requests as an issue on this repository.

## License
AriaSQL is licensed under the AGPL-3.0 license.
