package executor

import (
	"ariasql/catalog"
	"ariasql/core"
	"ariasql/parser"
	"os"
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
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}
}

func TestStmt2(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

}

func TestStmt3(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

}

func TestStmt4(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Jane Doe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name = 'John Doe';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt5(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name = 'John Doe';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt6(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt7(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255), age INT);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)

	ast, err = p.Parse()

	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username, age) VALUES (1, 'jdoe', 4), (2, 'adoe', 3);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)

	ast, err = p.Parse()

	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users WHERE age+1 = 5;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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
| 5   | 1       | 'jdoe'   |
+-----+---------+----------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return
	}

}

func TestStmt8(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id+1 = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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
| 2             | 'Hello World 2' | 2             | 2             | 'jdoe'         |
+---------------+-----------------+---------------+---------------+----------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt9(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id = 1;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt10(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE test.name = 'John Doe';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+
| test.id | test.name  |
+---------+------------+
| 1       | 'John Doe' |
+---------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt11(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'admin');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id OR users.username = 'admin';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt12(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT, name CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (id, name) VALUES (1, 'John Doe'), (2, 'Jane Doe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM test WHERE name IN ('John Doe', 'Jane Doe');
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+
| test.id | test.name  |
+---------+------------+
| 1       | 'John Doe' |
| 2       | 'Jane Doe' |
+---------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}
}

func TestStmt13(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt14(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM users WHERE username IS NULL;
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+----------------+
| users.user_id | users.username |
+---------------+----------------+
| 2             | <nil>          |
+---------------+----------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt15(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT UNIQUE NOT NULL, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT UNIQUE NOT NULL, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
		SELECT * FROM users WHERE username IS NOT NULL;
	`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------------+----------------+
| users.user_id | users.username |
+---------------+----------------+
| 1             | 'jdoe'         |
+---------------+----------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt16(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE 'A%';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------------+
| test.id | test.name     |
+---------+---------------+
| 2       | 'Alex Padula' |
+---------+---------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt17(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE '%Padula';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------------+
| test.id | test.name     |
+---------+---------------+
| 2       | 'Alex Padula' |
+---------+---------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt18(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE '%Pad%';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------------+
| test.id | test.name     |
+---------+---------------+
| 2       | 'Alex Padula' |
+---------+---------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt19(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name LIKE 'A%la';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------------+
| test.id | test.name     |
+---------+---------------+
| 2       | 'Alex Padula' |
+---------+---------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt20(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id IN (1);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt21(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users, posts WHERE users.user_id = posts.user_id AND users.user_id BETWEEN 1 AND 2;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt22(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name NOT LIKE 'A%';
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+
| test.id | test.name  |
+---------+------------+
| 1       | 'John Doe' |
+---------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt23(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE name NOT IN ('Alex Padula');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+
| test.id | test.name  |
+---------+------------+
| 1       | 'John Doe' |
+---------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt24(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id NOT BETWEEN 2 AND 3;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+--------------+
| test.id | test.name    |
+---------+--------------+
| 1       | 'John Doe'   |
| 4       | 'Alex Smith' |
+---------+--------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt25(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id IN (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+------------+
| test.id | test.name  |
+---------+------------+
| 1       | 'John Doe' |
+---------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt26(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id NOT IN (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+---------+---------------+
| test.id | test.name     |
+---------+---------------+
| 2       | 'Alex Padula' |
| 3       | 'John Smith'  |
| 4       | 'Alex Smith'  |
+---------+---------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt27(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE id = (SELECT id FROM test2 WHERE name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//	expect := `+---------+---------------+
	//| test.id | test.name     |
	//+---------+---------------+
	//| 2       | 'Alex Padula' |
	//| 3       | 'John Smith'  |
	//| 4       | 'Alex Smith'  |
	//+---------+---------------+
	//`

	// Uncomment this after select list implementation
	//if string(ex.resultSetBuffer) != expect {
	//	t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
	//	return
	//
	//}

}

func TestStmt28(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE test2 (id INT SEQUENCE NOT NULL UNIQUE, name CHAR(255) UNIQUE);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test (name) VALUES ('John Doe'),('Alex Padula'),('John Smith'), ('Alex Smith');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO test2 (name) VALUES ('Dog'),('Cat'),('Turtle'),('Snake');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM test WHERE EXISTS (SELECT * FROM test2 WHERE test.id = test2.id AND name = 'Dog');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := `+----+-------+----------+------------+
| id | name  | test2.id | test2.name |
+----+-------+----------+------------+
| 1  | 'Dog' | 1        | 'Dog'      |
+----+-------+----------+------------+
`

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt29(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT title FROM posts;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt30(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE posts (post_id INT, title CHAR(255), user_id INT NOT NULL);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO posts (post_id, title, user_id) VALUES (1, 'Hello World', 1), (2, 'Hello World 2', 2);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT COUNT(*) FROM posts;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt31(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT SUM(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt32(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT AVG(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt33(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT MIN(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt34(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT MAX(money) FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt35(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, money FLOAT(10,2));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, money) VALUES (1, 100.00), (2, 200.00), (3, 300.00), (4, 400.00), (5, 500.00);
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	// GROUP BY
	stmt = []byte(`
	SELECT user_id, SUM(money) FROM users GROUP BY user_id HAVING SUM(money) > 200;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	expect := ``

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt36(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users ORDER BY username ASC;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt37(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	SELECT * FROM users ORDER BY username ASC LIMIT 2 OFFSET 2;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return

	}

}

func TestStmt38(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	UPDATE users SET username = 'updated_username' WHERE user_id = 1;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return
	}

	ex.Clear()

	stmt = []byte(`
	SELECT * FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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
| 1       | 'updated_username' |
| 2       | 'adoe'             |
| 3       | 'bdoe'             |
| 4       | 'cdoe'             |
| 5       | 'ddoe'             |
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return
	}

}

func TestStmt39(t *testing.T) {
	defer os.RemoveAll("./test/")

	// Create a new AriaSQL instance
	aria := core.New(&core.Config{
		DataDir: "./test/", // For now, can be set in aria config file
	})

	aria.Catalog = catalog.New(aria.Config.DataDir)

	if err := aria.Catalog.Open(); err != nil {
		t.Fatal(err)
		return
	}

	aria.Channels = make([]*core.Channel, 0)
	aria.ChannelsLock = &sync.Mutex{}

	ch := aria.OpenChannel()
	ex := New(aria, ch)

	stmt := []byte(`
	CREATE DATABASE test;
`)

	lexer := parser.NewLexer(stmt)

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
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
	}

	stmt = []byte(`
	USE test;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	CREATE TABLE users (user_id INT, username CHAR(255));
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	INSERT INTO users (user_id, username) VALUES (1, 'jdoe'), (2, 'adoe'), (3, 'bdoe'), (4, 'cdoe'), (5, 'ddoe'), (6, 'edoe'), (7, 'fdoe'), (8, 'gdoe'), (9, 'hdoe'), (10, 'idoe'), (11, 'jdoe'), (12, 'kdoe'), (13, 'ldoe'), (14, 'mdoe'), (15, 'ndoe'), (16, 'odo');
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
	if err != nil {
		t.Fatal(err)
		return
	}

	err = ex.Execute(ast)
	if err != nil {
		t.Fatal(err)
		return
	}

	//log.Println(string(ex.resultSetBuffer))
	// result should be empty
	if len(ex.resultSetBuffer) != 0 {
		t.Fatalf("expected empty result set buffer, got %s", string(ex.resultSetBuffer))
		return
	}

	stmt = []byte(`
	DELETE FROM users WHERE user_id = 1;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return
	}

	ex.Clear()

	stmt = []byte(`
	SELECT * FROM users;
`)

	lexer = parser.NewLexer(stmt)

	p = parser.NewParser(lexer)
	ast, err = p.Parse()
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

	if string(ex.resultSetBuffer) != expect {
		t.Fatalf("expected %s, got %s", expect, string(ex.resultSetBuffer))
		return
	}

}
