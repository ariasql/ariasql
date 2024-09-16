// Package executor tests
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
package executor

import (
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/parser"
	"ariasql/wal"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	ex := New(nil, nil)
	if ex == nil {
		t.Fatal("expected non-nil executor")
	}
}

func TestStmt(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")

	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}
}

func TestStmt1(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")

	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT 5+(4*33);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `
+-----+
| 137 |
+-----+
| 137 |
+-----+
`

	if strings.TrimSpace(string(ex.ResultSetBuffer)) != strings.TrimSpace(expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}
}

func TestStmt2(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}
	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt3(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt4(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Jane Doe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name = 'John Doe';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt5(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id = 1 OR name = 'Jane Doe';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
| 2  | 'Jane Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt6(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT UNIQUE NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+-----------------+---------------+---------------+----------------+
| posts.post_id | posts.title     | posts.user_id | users.user_id | users.username |
+---------------+-----------------+---------------+---------------+----------------+
| 1             | 'Hello World'   | 1             | 1             | 'jdoe'         |
| 2             | 'Hello World 2' | 2             | 2             | 'adoe'         |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt7(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), age INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()

	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, age) VALUES (1, 'jdoe', 4), (2, 'adoe', 3);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()

	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE age+1 = 5;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----+---------+----------+
| age | user_id | username |
+-----+---------+----------+
| 4   | 1       | 'jdoe'   |
+-----+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt8(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT NOT NULL UNIQUE, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT NOT NULL UNIQUE, title CHAR(255), user_id INT NOT NULL UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+-----------------+---------------+---------------+----------------+
| posts.post_id | posts.title     | posts.user_id | users.user_id | users.username |
+---------------+-----------------+---------------+---------------+----------------+
| 1             | 'Hello World'   | 1             | 1             | 'jdoe'         |
| 2             | 'Hello World 2' | 2             | 2             | 'adoe'         |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt9(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id = 1;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+---------------+---------------+---------------+----------------+
| posts.post_id | posts.title   | posts.user_id | users.user_id | users.username |
+---------------+---------------+---------------+---------------+----------------+
| 1             | 'Hello World' | 1             | 1             | 'jdoe'         |
+---------------+---------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt10(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE test.name = 'John Doe';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt11(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'admin');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id OR users.username = 'admin';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+-----------------+---------------+---------------+----------------+
| posts.post_id | posts.title     | posts.user_id | users.user_id | users.username |
+---------------+-----------------+---------------+---------------+----------------+
| 1             | 'Hello World'   | 1             | 1             | 'jdoe'         |
| 2             | 'Hello World 2' | 2             | 2             | 'adoe'         |
| <nil>         | <nil>           | <nil>         | 3             | 'admin'        |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt12(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM test WHERE name IN ('John Doe', 'Jane Doe');
	`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
| 2  | 'Jane Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt13(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+-----------------+---------------+---------------+----------------+
| posts.post_id | posts.title     | posts.user_id | users.user_id | users.username |
+---------------+-----------------+---------------+---------------+----------------+
| 1             | 'Hello World'   | 1             | 1             | 'jdoe'         |
| 2             | 'Hello World 2' | 2             | 2             | 'adoe'         |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt14(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	//result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM users WHERE username IS NULL;
	`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| user_id | username |
+---------+----------+
| 2       | <nil>    |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt15(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	//result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM users WHERE username IS NOT NULL;
	`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| user_id | username |
+---------+----------+
| 1       | 'jdoe'   |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt16(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE 'A%';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+---------------+
| id | name          |
+----+---------------+
| 2  | 'Alex Padula' |
+----+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt17(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE '%Padula';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+---------------+
| id | name          |
+----+---------------+
| 2  | 'Alex Padula' |
+----+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt18(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE '%Pad%';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+---------------+
| id | name          |
+----+---------------+
| 2  | 'Alex Padula' |
+----+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt19(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE 'A%la';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+---------------+
| id | name          |
+----+---------------+
| 2  | 'Alex Padula' |
+----+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt20(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id in (1);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+---------------+---------------+---------------+----------------+
| posts.post_id | posts.title   | posts.user_id | users.user_id | users.username |
+---------------+---------------+---------------+---------------+----------------+
| 1             | 'Hello World' | 1             | 1             | 'jdoe'         |
+---------------+---------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt21(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id BETWEEN 1 AND 2;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+-----------------+---------------+---------------+----------------+
| posts.post_id | posts.title     | posts.user_id | users.user_id | users.username |
+---------------+-----------------+---------------+---------------+----------------+
| 1             | 'Hello World'   | 1             | 1             | 'jdoe'         |
| 2             | 'Hello World 2' | 2             | 2             | 'adoe'         |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt22(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name NOT LIKE 'A%';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt23(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name NOT IN ('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt24(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id NOT BETWEEN 2 AND 3;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+--------------+
| id | name         |
+----+--------------+
| 1  | 'John Doe'   |
| 4  | 'Alex Smith' |
+----+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt25(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id IN (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt26(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id NOT IN (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+---------------+
| id | name          |
+----+---------------+
| 2  | 'Alex Padula' |
| 3  | 'John Smith'  |
| 4  | 'Alex Smith'  |
+----+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt27(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id = (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+------------+
| id | name       |
+----+------------+
| 1  | 'John Doe' |
+----+------------+
`

	// Uncomment this after select list implementation
	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt28(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
SELECT * 
FROM test 
WHERE EXISTS (
    SELECT *
    FROM test2 
    WHERE test.id = test2.id 
    AND test2.name = 'Dog'
);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+----------+------------+
| test.id | test.name  | test2.id | test2.name |
+---------+------------+----------+------------+
| 1       | 'John Doe' | 1        | 'Dog'      |
+---------+------------+----------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt29(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT title FROM posts;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----------------+
| title           |
+-----------------+
| 'Hello World'   |
| 'Hello World 2' |
+-----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt30(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT COUNT(*) FROM posts;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+
| COUNT |
+-------+
| 2     |
+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt31(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT SUM(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------+
| SUM  |
+------+
| 1500 |
+------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt32(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT AVG(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----+
| AVG |
+-----+
| 300 |
+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt33(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT MIN(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----+
| MIN |
+-----+
| 100 |
+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt34(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT MAX(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----+
| MAX |
+-----+
| 500 |
+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt35(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT user_id, SUM(money) FROM users GROUP BY user_id HAVING SUM(money) > 200 ORDER BY user_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+---------+
| money | user_id |
+-------+---------+
| 500   | 5       |
| 400   | 4       |
| 300   | 3       |
+-------+---------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt36(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users ORDER BY username ASC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| user_id | username |
+---------+----------+
| 2       | 'adoe'   |
| 3       | 'bdoe'   |
| 4       | 'cdoe'   |
| 5       | 'ddoe'   |
| 6       | 'edoe'   |
| 7       | 'fdoe'   |
| 8       | 'gdoe'   |
| 9       | 'hdoe'   |
| 10      | 'idoe'   |
| 1       | 'jdoe'   |
| 11      | 'jdoe'   |
| 12      | 'kdoe'   |
| 13      | 'ldoe'   |
| 14      | 'mdoe'   |
| 15      | 'ndoe'   |
| 16      | 'odo'    |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt37(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users ORDER BY username ASC LIMIT 2 OFFSET 2;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| user_id | username |
+---------+----------+
| 4       | 'cdoe'   |
| 5       | 'ddoe'   |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt38(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	UPDATE users SET username = 'updated_username' WHERE user_id = 6 OR user_id = 4;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+
| RowsAffected |
+--------------+
| 2            |
+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

	ex.Clear()

	stmt = []byte(`
	SELECT * FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect = `+---------+--------------------+
| user_id | username           |
+---------+--------------------+
| 1       | 'jdoe'             |
| 2       | 'adoe'             |
| 3       | 'bdoe'             |
| 4       | 'updated_username' |
| 5       | 'ddoe'             |
| 6       | 'updated_username' |
| 7       | 'fdoe'             |
| 8       | 'gdoe'             |
| 9       | 'hdoe'             |
| 10      | 'idoe'             |
| 11      | 'jdoe'             |
| 12      | 'kdoe'             |
| 13      | 'ldoe'             |
| 14      | 'mdoe'             |
| 15      | 'ndoe'             |
| 16      | 'odo'              |
+---------+--------------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt39(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	DELETE FROM users WHERE user_id = 1;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+
| RowsAffected |
+--------------+
| 1            |
+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

	ex.Clear()

	stmt = []byte(`
	SELECT * FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect = `+---------+----------+
| user_id | username |
+---------+----------+
| 2       | 'adoe'   |
| 3       | 'bdoe'   |
| 4       | 'cdoe'   |
| 5       | 'ddoe'   |
| 6       | 'edoe'   |
| 7       | 'fdoe'   |
| 8       | 'gdoe'   |
| 9       | 'hdoe'   |
| 10      | 'idoe'   |
| 11      | 'jdoe'   |
| 12      | 'kdoe'   |
| 13      | 'ldoe'   |
| 14      | 'mdoe'   |
| 15      | 'ndoe'   |
| 16      | 'odo'    |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt40(t *testing.T) {

	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	BEGIN;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	UPDATE users SET username = 'updated_username' WHERE user_id = 1;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	COMMIT;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE user_id = 1;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+--------------------+
| user_id | username           |
+---------+--------------------+
| 1       | 'updated_username' |
+---------+--------------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt41(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users u, posts p WHERE u.user_id = p.user_id;
`) // inner join/implied join

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----------+-----------------+-----------+-----------+------------+
| p.post_id | p.title         | p.user_id | u.user_id | u.username |
+-----------+-----------------+-----------+-----------+------------+
| 1         | 'Hello World'   | 1         | 1         | 'jdoe'     |
| 2         | 'Hello World 2' | 2         | 2         | 'adoe'     |
+-----------+-----------------+-----------+-----------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt42(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'frankenstein'), (2, 'frankenstein'), (3, 'drako');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT DISTINCT username FROM users;
`) // inner join/implied join

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------------+
| username       |
+----------------+
| 'frankenstein' |
| 'drako'        |
+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt43(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE y (x INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	INSERT INTO y (x) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM y ORDER BY x DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+
| x  |
+----+
| 20 |
| 19 |
| 18 |
| 17 |
| 16 |
| 15 |
| 14 |
| 13 |
| 12 |
| 11 |
| 10 |
| 9  |
| 8  |
| 7  |
| 6  |
| 5  |
| 4  |
| 3  |
| 2  |
| 1  |
+----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

// setupToRecover sets up a test database to be used for recovery testing
func setupToRecover() error {
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		return err
	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err = aria.Catalog.Open(); err != nil {
		return err

	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
		CREATE DATABASE test;
	`)

	lexer := parser.NewLexer(stmt)

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		return err

	}

	err = ex.Execute(ast)
	if err != nil {
		return err
	}

	stmt = []byte(`
		USE test;
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		return err

	}

	err = ex.Execute(ast)
	if err != nil {
		return err
	}

	stmt = []byte(`
		CREATE TABLE test (x INT);
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		return err

	}

	err = ex.Execute(ast)
	if err != nil {
		return err
	}

	stmt = []byte(`
		INSERT INTO test (x) VALUES (1);
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		return err

	}

	err = ex.Execute(ast)
	if err != nil {
		return err
	}

	return nil

}

func TestExecutor_Recover(t *testing.T) {
	defer os.RemoveAll("./test")
	err := setupToRecover()
	if err != nil {
		t.Errorf("setupToRecover failed: %v", err)
		return
	}

	wal, err := wal.OpenWAL("./test/wal.dat", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Errorf("OpenWAL failed: %v", err)
		return
	}

	defer wal.Close()

	asts, err := wal.RecoverASTs()
	if err != nil {
		t.Fatal(err)
		return
	}

	wal.Close()

	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})

	if err != nil {
		t.Errorf("core.New failed: %v", err)
		return
	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err = aria.Catalog.Open(); err != nil {
		t.Errorf("aria.Catalog.Open failed: %v", err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)

	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")

	ch := aria.OpenChannel(user)

	ex := New(aria, ch)
	ex.SetRecover(true)

	err = ex.Recover(asts)
	if err != nil {
		t.Fatal(err)
		return
	}

	aria, err = core.New(&core.Config{
		DataDir: "./test",
	})

	if err != nil {
		t.Errorf("core.New failed: %v", err)
	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err = aria.Catalog.Open(); err != nil {
		t.Errorf("aria.Catalog.Open failed: %v", err)
	}

	aria.Channels = make([]*core.Channel, 0)

	aria.ChannelsLock = &sync.Mutex{}

	user = aria.Catalog.GetUser("admin")

	ch = aria.OpenChannel(user)

	ex = New(aria, ch)

	stmt := []byte(`
		USE test;
	`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)

	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)

	stmt = []byte(`
			SELECT * FROM test;
		`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)

	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---+
| x |
+---+
| 1 |
+---+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}
}

func TestStmt44(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	create user alex identified by 'password';
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	show users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+
| User  |
+-------+
| admin |
| alex  |
+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt45(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	create user alex identified by 'password';
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	grant all ON *.* to alex;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	grant select, insert ON db1.* to alex;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	show grants;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------------------------+-------+
| Grants                         | User  |
+--------------------------------+-------+
| *.*: ALL                       | admin |
| *.*: ALL,db1.*: SELECT, INSERT | alex  |
+--------------------------------+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt46(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	SELECT 1+1 AS result;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------+
| result |
+--------+
| 2      |
+--------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}
}

func TestStmt47(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	CREATE TABLE y(x INT, n INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	INSERT INTO y (x, n) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	SELECT n + 1 + 1 as R from y;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+
| R  |
+----+
| 3  |
| 4  |
| 5  |
| 6  |
| 7  |
| 8  |
| 9  |
| 10 |
| 11 |
| 12 |
| 13 |
| 14 |
| 15 |
| 16 |
| 17 |
| 18 |
| 19 |
| 20 |
| 21 |
| 22 |
+----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}
}

func TestStmt48(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	CREATE TABLE y(x INT, n INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	INSERT INTO y (x, n) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	SELECT count(*)+1+1 AS R from y;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+
| R  |
+----+
| 22 |
+----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}
}

func TestStmt49(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	defer aria.Close()

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	CREATE TABLE y(x INT, n INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	INSERT INTO y (x, n) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	stmt = []byte(`
	SELECT sum(n) AS RESPONSE from y;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------+
| RESPONSE |
+----------+
| 210      |
+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}
}

func TestStmt50(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT u.* FROM users u, posts p WHERE u.user_id = p.user_id AND u.user_id BETWEEN 1 AND 2;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----------+------------+
| u.user_id | u.username |
+-----------+------------+
| 1         | 'jdoe'     |
| 2         | 'adoe'     |
+-----------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt51(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'ALEX'), (2, 'JDOE');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username FROM users WHERE UPPER(username) = 'ALEX';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------+
| username |
+----------+
| 'ALEX'   |
+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt52(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), has_dog CHAR(9));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, has_dog) VALUES (1, 'ALEX', 'true'), (2, 'JDOE', 'false');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, has_dog FROM users WHERE CAST(has_dog AS BOOL) = true;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| has_dog | username |
+---------+----------+
| true    | 'ALEX'   |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt53(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), has_dog CHAR(9));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, has_dog) VALUES (1, 'alex', NULL), (2, 'dave', NULL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, has_dog FROM users WHERE COALESCE(has_dog, 'true') = 'true';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----------+
| has_dog | username |
+---------+----------+
| 'true'  | 'alex'   |
| 'true'  | 'dave'   |
+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt54(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), has_dog BOOL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, has_dog) VALUES (1, '  alex  ', true), (2, '  dave  ', false);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE TRIM(username) = 'alex';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------+----------+
| has_dog | user_id | username |
+---------+---------+----------+
| true    | 1       | 'alex'   |
+---------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt55(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), has_dog BOOL);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, has_dog) VALUES (1, 'alex', true), (2, 'joe', false);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE LENGTH(username) = 3;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------+----------+
| has_dog | user_id | username |
+---------+---------+----------+
| false   | 2       | 3        |
+---------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt56(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), money DECIMAL(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'alex', 2.77), (2, 'joe', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE ROUND(money) = 3;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+---------+----------+
| money | user_id | username |
+-------+---------+----------+
| 3     | 1       | 'alex'   |
+-------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt57(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), money DECIMAL(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'alex', 2.77), (2, 'joe', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE CONCAT(username, '!') = 'alex!';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+---------+----------+
| money | user_id | username |
+-------+---------+----------+
| 2.77  | 1       | 'alex!'  |
+-------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt58(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), money DECIMAL(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'alex', 2.77), (2, 'joe', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE SUBSTRING(username, 1, 3) = 'ale';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+---------+----------+
| money | user_id | username |
+-------+---------+----------+
| 2.77  | 1       | 'ale'    |
+-------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt59(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), money DECIMAL(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'alex', 2.77), (2, 'joe', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE POSITION('le' IN username) = 3;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+---------+----------+
| money | user_id | username |
+-------+---------+----------+
| 2.77  | 1       | 3        |
+-------+---------+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt61(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'alex', 2.77), (2, 'joe', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT UPPER(username) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------+
| username |
+----------+
| 'ALEX'   |
| 'JOE'    |
+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt62(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'AlEx', 2.77), (2, 'JoE', 33.44);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT LOWER(username) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------+
| username |
+----------+
| 'alex'   |
| 'joe'    |
+----------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt63(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, money) VALUES (1, 'AlEx', '2.77'), (2, 'JoE', '33.44');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT CAST(money AS DECIMAL) AS mola FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+
| mola  |
+-------+
| 2.77  |
| 33.44 |
+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt64(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DECIMAL(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'Jonathan'), (2, 'Joe'), (3, 'Jane');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT user_id, username, COALESCE(money, 'no mola') AS mola FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----------+---------+------------+
| mola      | user_id | username   |
+-----------+---------+------------+
| 'no mola' | 1       | 'Jonathan' |
| 'no mola' | 2       | 'Joe'      |
| 'no mola' | 3       | 'Jane'     |
+-----------+---------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt65(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username) VALUES ('Jonathan'), ('Joe'), ('Jane');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT REVERSE(username) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+
| username   |
+------------+
| 'nahtanoJ' |
| 'eoJ'      |
| 'enaJ'     |
+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt66(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22.44), ('Joe', 38.78), ('Jane', 21.10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, ROUND(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------+------------+
| money | username   |
+-------+------------+
| 22    | 'Jonathan' |
| 39    | 'Joe'      |
| 21    | 'Jane'     |
+-------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt67(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22.44), ('Joe', 38.78), ('Jane', 21.10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, POSITION(username IN 'than') AS pos FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-----+------------+
| pos | username   |
+-----+------------+
| 5   | 'Jonathan' |
| -1  | 'Joe'      |
| -1  | 'Jane'     |
+-----+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt68(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22.44), ('Joe', 38.78), ('Jane', 21.10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, LENGTH(username) AS username_len FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+--------------+
| username   | username_len |
+------------+--------------+
| 'Jonathan' | 8            |
| 'Joe'      | 3            |
| 'Jane'     | 4            |
+------------+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt69(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('  Jonathan', 22.44), ('   Joe', 38.78), ('  Jane   ', 21.10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, TRIM(username) AS username_trim FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+---------------+
| username     | username_trim |
+--------------+---------------+
| '  Jonathan' | 'Jonathan'    |
| '   Joe'     | 'Joe'         |
| '  Jane   '  | 'Jane'        |
+--------------+---------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt70(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22.44), ('Joe', 38.78), ('Jane', 21.10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, SUBSTRING(username, 1, 2) AS username_subst FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+----------------+
| username   | username_subst |
+------------+----------------+
| 'Jonathan' | 'Jo'           |
| 'Joe'      | 'Jo'           |
| 'Jane'     | 'Ja'           |
+------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt71(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', '22.44'), ('Joe', '38.78'), ('Jane', '21.10');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, CONCAT('$', money) FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----------+------------+
| money    | username   |
+----------+------------+
| '$22.44' | 'Jonathan' |
| '$38.78' | 'Joe'      |
| '$21.10' | 'Jane'     |
+----------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt72(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22), ('Joe', 38), ('Jane', 21);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, CASE 
		WHEN money > 30 THEN 'rich class'
		WHEN money < 30 THEN 'poor class'
		ELSE 'middle class'
		END AS money_status
	FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+------------+
| money_status | username   |
+--------------+------------+
| 'rich class' | 'Jonathan' |
| 'poor class' | 'Joe'      |
| 'poor class' | 'Jane'     |
+--------------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt73(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22), ('Joe', 38), ('Jane', 21);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username, CASE 
		WHEN money > 30 THEN 'rich class'
		WHEN money < 30 THEN 'poor class'
		ELSE 'middle class'
		END
	FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+------------+
| case_result  | username   |
+--------------+------------+
| 'rich class' | 'Jonathan' |
| 'poor class' | 'Joe'      |
| 'poor class' | 'Jane'     |
+--------------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt74(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id UUID DEFAULT GENERATE_UUID, username CHAR(255), created_on DATETIME DEFAULT SYS_DATE, money DEC(10, 2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (username, money) VALUES ('Jonathan', 22), ('Joe', 38), ('Jane', 21);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT username
	FROM users
	WHERE CASE 
		WHEN money > 30 THEN 'rich class'
		WHEN money < 30 THEN 'poor class'
		ELSE 'middle class'
		END = 'poor class' 
		AND username = 'Jonathan';
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+
| username   |
+------------+
| 'Jonathan' |
+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}
}

func TestStmt75(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT PRIMARY KEY SEQUENCE, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (val) VALUES (1), (2), (3), (4), (5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	DECLARE @id INT; -- Declare a variable for the id
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	PRINT @id; -- Print the value of the id variable
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	// You should see nil printed to the console
}

func TestStmt76(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT PRIMARY KEY SEQUENCE, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (val) VALUES (1), (2), (3), (4), (5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	DECLARE @id INT; -- Declare a variable for the id
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	DECLARE id_cursor CURSOR FOR SELECT id FROM t;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	OPEN id_cursor;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	FETCH NEXT FROM id_cursor INTO @id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	PRINT @id; -- Print the value of the id variable
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	// should see 1 printed to the console

	stmt = []byte(`
	DEALLOCATE @id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	DEALLOCATE id_cursor;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}
}

func TestStmt77(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT PRIMARY KEY SEQUENCE, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (val) VALUES (1), (2), (3), (4), (5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	DECLARE @id INT; -- Declare a variable for the id
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	DECLARE id_cursor CURSOR FOR SELECT id FROM t;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	OPEN id_cursor;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	WHILE @@FETCH_STATUS = 0
	BEGIN
		FETCH NEXT FROM id_cursor INTO @id;
		UPDATE t SET val = val + 1 WHERE id = @id;
	END;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	//if len(ex.ResultSetBuffer) != 0 {
	//	t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	//}

	stmt = []byte(`
	SELECT * FROM t;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
	}

	expect := `+----+-----+
| id | val |
+----+-----+
| 1  | 2   |
| 2  | 3   |
| 3  | 4   |
| 4  | 5   |
| 5  | 6   |
+----+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt78(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	UPDATE users SET username = 'updated_username' WHERE user_id = 5;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------------+
| RowsAffected |
+--------------+
| 1            |
+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

	ex.Clear()

	stmt = []byte(`
	SELECT * FROM users;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect = `+---------+--------------------+
| user_id | username           |
+---------+--------------------+
| 1       | 'jdoe'             |
| 2       | 'adoe'             |
| 3       | 'bdoe'             |
| 4       | 'cdoe'             |
| 5       | 'updated_username' |
| 6       | 'edoe'             |
| 7       | 'fdoe'             |
| 8       | 'gdoe'             |
| 9       | 'hdoe'             |
| 10      | 'idoe'             |
| 11      | 'jdoe'             |
| 12      | 'kdoe'             |
| 13      | 'ldoe'             |
| 14      | 'mdoe'             |
| 15      | 'ndoe'             |
| 16      | 'odo'              |
+---------+--------------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return
	}

}

func TestStmt79(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT PRIMARY KEY SEQUENCE, val INT UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (val) VALUES (1), (2), (3), (4), (5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	UPDATE t SET val = val + 1;

`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	//if len(ex.ResultSetBuffer) != 0 {
	//	t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	//}

	stmt = []byte(`
	SELECT * FROM t;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
	}

	expect := `+----+-----+
| id | val |
+----+-----+
| 1  | 2   |
| 2  | 3   |
| 3  | 4   |
| 4  | 5   |
| 5  | 6   |
+----+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt80(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (id, val) VALUES (1,1), (2,2), (3,3), (4,4), (5,5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	SELECT * FROM t;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)

	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
	}

	expect := `+----+-----+
| id | val |
+----+-----+
| 1  | 1   |
| 2  | 2   |
| 3  | 3   |
| 4  | 4   |
| 5  | 5   |
+----+-----+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt81(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE PROCEDURE test_proc()
	BEGIN
		SELECT * FROM t;
	END;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	// Result set should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

}

func TestStmt82(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (id, val) VALUES (1,1), (2,2), (3,3), (4,4), (5,5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	EXPLAIN SELECT * FROM t;
	`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------+----+-----------+-------+
| column | io | operation | table |
+--------+----+-----------+-------+
| n/a    | 5  | FULL SCAN | t     |
+--------+----+-----------+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt83(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE t (id INT PRIMARY KEY SEQUENCE, val INT);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO t (val) VALUES (1), (2), (3), (4), (5);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	EXPLAIN SELECT * FROM t WHERE id = 4 AND val = 4;
	`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------+----+------------+-------+
| column | io | operation  | table |
+--------+----+------------+-------+
| id     | 2  | INDEX SCAN | t     |
+--------+----+------------+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt84(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT PRIMARY KEY SEQUENCE, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT PRIMARY KEY SEQUENCE, title CHAR(255), user_id INT NOT NULL UNIQUE);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	EXPLAIN SELECT * FROM users u, posts p WHERE u.user_id = p.user_id;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+----+------------+-------+
| column  | io | operation  | table |
+---------+----+------------+-------+
| user_id | 2  | INDEX SCAN | u     |
| user_id | 2  | INDEX SCAN | p     |
+---------+----+------------+-------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

// Benchmark 10,000 insertions
func BenchmarkStmt85(b *testing.B) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		b.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		b.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	b.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		b.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		b.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		b.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	b.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		b.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		b.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		b.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL SEQUENCE, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)
	b.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		b.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		b.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		b.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	for i := 0; i < b.N; i++ {
		stmt = []byte(`
	INSERT INTO users (username) VALUES ( 'jdoe'), ('adoe');
`)

		lexer = parser.NewLexer(stmt)
		b.Log(string(stmt))

		p = parser.NewParser(lexer)
		ast, err = p.Parse()
		if err != nil {
			b.Fatal(err)
			return
		}

		err = ex.Execute(ast)
		if err != nil {
			b.Fatal(err)
			return
		}

		//log.Println(string(ex.resultSetBuffer))
		// result should be empty
		if len(ex.ResultSetBuffer) != 0 {
			b.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
			return
		}
	}

}

func TestStmt85(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE Sales (
		SaleID INT PRIMARY KEY,
		Product CHAR(50),
		Amount DECIMAL(10, 2),
		SaleDate DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO Sales (SaleID, Product, Amount, SaleDate) VALUES
	(1, 'Widget', 25.00, '2024-09-01'),
	(2, 'Gadget', 15.50, '2024-09-01'),
	(3, 'Widget', 25.00, '2024-09-02'),
	(4, 'Widget', 30.00, '2024-09-02'),
	(5, 'Gadget', 15.50, '2024-09-03'),
	(6, 'Widget', 20.00, '2024-09-03'),
	(7, 'Gadget', 20.00, '2024-09-04');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT SUM(Amount) AS TotalSales FROM Sales;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+
| TotalSales |
+------------+
| 150        |
+------------+
`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt86(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE Sales (
		SaleID INT PRIMARY KEY,
		Product CHAR(50),
		Amount DECIMAL(10, 2),
		SaleDate DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO Sales (SaleID, Product, Amount, SaleDate) VALUES
	(1, 'Widget', 25.00, '2024-09-01'),
	(2, 'Gadget', 15.50, '2024-09-01'),
	(3, 'Widget', 25.00, '2024-09-02'),
	(4, 'Widget', 30.00, '2024-09-02'),
	(5, 'Gadget', 15.50, '2024-09-03'),
	(6, 'Widget', 20.00, '2024-09-03'),
	(7, 'Gadget', 20.00, '2024-09-04');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM Sales;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------+----------+--------------+--------+
| Amount | Product  | SaleDate     | SaleID |
+--------+----------+--------------+--------+
| 25     | 'Widget' | '2024-09-01' | 1      |
| 15.5   | 'Gadget' | '2024-09-01' | 2      |
| 25     | 'Widget' | '2024-09-02' | 3      |
| 30     | 'Widget' | '2024-09-02' | 4      |
| 15.5   | 'Gadget' | '2024-09-03' | 5      |
| 20     | 'Widget' | '2024-09-03' | 6      |
| 20     | 'Gadget' | '2024-09-04' | 7      |
+--------+----------+--------------+--------+
`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

	//log.Println(string(ex.ResultSetBuffer))

}

func TestStmt87(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE Sales (
		SaleID INT PRIMARY KEY,
		Product CHAR(50),
		Amount DECIMAL(10, 2),
		SaleDate DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO Sales (SaleID, Product, Amount, SaleDate) VALUES
	(1, 'Widget', 25.00, '2024-09-01'),
	(2, 'Gadget', 15.50, '2024-09-01'),
	(3, 'Widget', 25.00, '2024-09-02'),
	(4, 'Widget', 30.00, '2024-09-02'),
	(5, 'Gadget', 15.50, '2024-09-03'),
	(6, 'Widget', 20.00, '2024-09-03'),
	(7, 'Gadget', 20.00, '2024-09-04');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT SaleID, Amount, Product FROM Sales WHERE Amount BETWEEN 20 AND 30 ORDER BY Amount DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+--------+----------+--------+
| Amount | Product  | SaleID |
+--------+----------+--------+
| 30     | 'Widget' | 4      |
| 25     | 'Widget' | 3      |
| 25     | 'Widget' | 1      |
| 20     | 'Gadget' | 7      |
| 20     | 'Widget' | 6      |
+--------+----------+--------+
`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

	//log.Println(string(ex.ResultSetBuffer))

}

func TestStmt88(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE x (
		y SMALLINT
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO x (y) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	ex.json = true

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM x WHERE y BETWEEN 3 AND 7 ORDER BY y DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `[{"y":7},{"y":6},{"y":5},{"y":4},{"y":3}]`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

	//log.Println(string(ex.ResultSetBuffer))

}

func TestStmt89(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE x (
		y CHAR(1)
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO x (y) VALUES ('a'), ('b'), ('c'), ('d'), ('e'), ('f'), ('g'), ('h'), ('i'), ('j');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	ex.json = true

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM x WHERE y BETWEEN 'c' AND 'g' ORDER BY y DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `[{"y":"g"},{"y":"f"},{"y":"e"},{"y":"d"},{"y":"c"}]`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

	//log.Println(string(ex.ResultSetBuffer))

}

func TestStmt90(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE x (
		y CHAR(1),
	    b BOOLEAN DEFAULT FALSE,
	    c DATETIME DEFAULT SYS_DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO x (y,b) VALUES ('a',true), ('b',false), ('c',true), ('d',false), ('e',true), ('f',false), ('g',true), ('h',false), ('i',true), ('j',false);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	ex.json = true

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT b, y FROM x WHERE b = true ORDER BY y DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `[{"b":true,"y":"i"},{"b":true,"y":"g"},{"b":true,"y":"e"},{"b":true,"y":"c"},{"b":true,"y":"a"}]`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

	//log.Println(string(ex.ResultSetBuffer))

}

func TestStmt91(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE Sales (
		SaleID INT PRIMARY KEY,
		Product CHAR(50),
		Amount DECIMAL(10, 2),
		SaleDate DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO Sales (SaleID, Product, Amount, SaleDate) VALUES
	(1, 'Widget', 25.00, '2024-09-01'),
	(2, 'Gadget', 15.50, '2024-09-01'),
	(3, 'Widget', 25.00, '2024-09-02'),
	(4, 'Widget', 30.00, '2024-09-02'),
	(5, 'Gadget', 15.50, '2024-09-03'),
	(6, 'Widget', 20.00, '2024-09-03'),
	(7, 'Gadget', 20.00, '2024-09-04');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT SUM(Amount)+1*22+(88 /2) AS TotalSales FROM Sales;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+
| TotalSales |
+------------+
| 216        |
+------------+
`

	if !strings.Contains(string(ex.ResultSetBuffer), expect) {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
	}

}

func TestStmt92(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT user_id FROM users GROUP BY user_id HAVING SUM(money) > 200 ORDER BY user_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+
| user_id |
+---------+
| 5       |
| 4       |
| 3       |
+---------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt93(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT user_id FROM users GROUP BY user_id HAVING SUM(money) > 200 ORDER BY user_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+
| user_id |
+---------+
| 5       |
| 4       |
| 3       |
+---------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt94(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE orders (
		order_id INT,
		customer_id INT,
		order_date DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO orders (order_id, customer_id, order_date) VALUES
	(1, 1, '2024-01-01'),
	(2, 1, '2024-01-15'),
	(3, 2, '2024-01-10'),
	(4, 2, '2024-01-20'),
	(5, 3, '2024-01-05'),
	(6, 3, '2024-01-25'),
	(7, 3, '2024-01-30');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT customer_id, COUNT(order_id) AS total_orders
	FROM orders
	GROUP BY customer_id
	HAVING COUNT(order_id) > 2
	ORDER BY customer_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+-------------+--------------+
| customer_id | total_orders |
+-------------+--------------+
| 3           | 3            |
+-------------+--------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt95(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE sales (
    sale_id INT,
    salesperson_id INT,
    sales_amount DECIMAL(10, 2),
    sale_date DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO sales (sale_id, salesperson_id, sales_amount, sale_date) VALUES
	(1, 1, 150.00, '2024-01-01'),
	(2, 1, 200.00, '2024-01-10'),
	(3, 2, 300.00, '2024-01-15'),
	(4, 2, 250.00, '2024-01-20'),
	(5, 3, 100.00, '2024-01-25'),
	(6, 3, 400.00, '2024-01-30'),
	(7, 4, 500.00, '2024-02-05'),
	(8, 4, 100.00, '2024-02-10');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT salesperson_id, AVG(sales_amount) AS avg_sales_amount
	FROM sales
	GROUP BY salesperson_id
	HAVING AVG(sales_amount) > 200
	ORDER BY salesperson_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------------+----------------+
| avg_sales_amount | salesperson_id |
+------------------+----------------+
| 300              | 4              |
| 250              | 3              |
| 275              | 2              |
+------------------+----------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt96(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE projects (
		project_id INT,
		project_manager_id INT,
		budget DECIMAL(10, 2),
		start_date DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO projects (project_id, project_manager_id, budget, start_date) VALUES
	(1, 1, 5000.00, '2024-01-01'),
	(2, 1, 7000.00, '2024-02-15'),
	(3, 2, 12000.00, '2024-01-10'),
	(4, 2, 15000.00, '2024-03-20'),
	(5, 3, 8000.00, '2024-01-25'),
	(6, 3, 9000.00, '2024-02-10'),
	(7, 4, 20000.00, '2024-02-05'),
	(8, 4, 15000.00, '2024-03-10');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT project_manager_id, MAX(budget) AS max_budget
	FROM projects
	GROUP BY project_manager_id
	HAVING MAX(budget) > 15000
	ORDER BY project_manager_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+------------+--------------------+
| max_budget | project_manager_id |
+------------+--------------------+
| 20000      | 4                  |
+------------+--------------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt97(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE employees (
		employee_id INT,
		department_id INT,
		salary DECIMAL(10, 2),
		hire_date DATE
	);

`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO employees (employee_id, department_id, salary, hire_date) VALUES
	(1, 1, 50000.00, '2024-01-01'),
	(2, 1, 60000.00, '2024-02-15'),
	(3, 1, 45000.00, '2024-03-01'),
	(4, 2, 70000.00, '2024-01-10'),
	(5, 2, 75000.00, '2024-03-20'),
	(6, 3, 55000.00, '2024-01-25'),
	(7, 3, 50000.00, '2024-02-10'),
	(8, 4, 80000.00, '2024-02-05'),
	(9, 4, 90000.00, '2024-03-10');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT department_id, MIN(salary) AS min_salary
	FROM employees
	GROUP BY department_id
	HAVING MIN(salary) > 55000
	ORDER BY department_id DESC;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+------------+
| department_id | min_salary |
+---------------+------------+
| 4             | 80000      |
| 2             | 70000      |
+---------------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}

func TestStmt98(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria, err := core.New(&core.Config{
		DataDir: "./test",
	})
	if err != nil {
		t.Fatal(err)
		return

	}

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	defer aria.Close()

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	user := aria.Catalog.GetUser("admin")
	ch := aria.OpenChannel(user)
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)
	t.Log(string(stmt))

	p := parser.NewParser(lexer)
	ast, err := p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE sales (
		sale_id INT,
		salesperson_id INT,
		sales_amount DECIMAL(10, 2),
		sale_date DATE
	);
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO sales (sale_id, salesperson_id, sales_amount, sale_date) VALUES
	(1, 1, 1000.00, '2024-01-01'),
	(2, 1, 1200.00, '2024-02-15'),
	(3, 2, 1500.00, '2024-01-10'),
	(4, 2, 800.00, '2024-03-20'),
	(5, 3, 2000.00, '2024-01-25'),
	(6, 3, 1800.00, '2024-02-10'),
	(7, 4, 700.00, '2024-02-05'),
	(8, 4, 800.00, '2024-03-10');
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.ResultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.ResultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT salesperson_id, SUM(sales_amount) AS total_sales, MAX(sales_amount) AS max_sale
	FROM sales
	GROUP BY salesperson_id
	HAVING SUM(sales_amount) > 2000 AND MAX(sales_amount) < 2000;
`)

	lexer = parser.NewLexer(stmt)
	t.Log(string(stmt))

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+------------+
| department_id | min_salary |
+---------------+------------+
| 4             | 80000      |
| 2             | 70000      |
+---------------+------------+
`

	if string(ex.ResultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.ResultSetBuffer))
		return

	}

}
