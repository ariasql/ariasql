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
echo -n "admin:admin" | base64

Above for example would be your base64 encoded auth string.

If you're using netcat simply pass the base64 encoded string as the first line.


```

