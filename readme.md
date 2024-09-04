<div>
    <h1 align="center"><img width="228" src="artwork/ariasql-logov1.png"></h1>
</div>

AriaSQL is an open source relational database server implementing SQL.
AriaSQL is still in the beginning stages of development and is not ready for production use.


## Features
- [x] Catalog implementation with tables, columns, and indexes
- [x] SQL1 lexer, parser
- [x] BTrees for indexes
- [x] Execution engine
- [x] TCP Server
- [x] User authentication and privileges
- [x] Transactions with WAL
- [x] Recovery
- [x] Subqueries

# todo
- [x] NOT expressions have to be handled in execution
- [x] Subqueries-EXISTS where applicable
- [x] Group by and having
- [x] Order by
- [x] Limit
- [x] Offset
- [x] update statement
- [x] delete statement
- [ ] transactions parsing, execution, and logic with WAL, and recover
- [ ] users and permissions
- [ ] server authentication
- [ ] more and more tests
- [ ] documentation
- [ ] procedures
- [ ] triggers