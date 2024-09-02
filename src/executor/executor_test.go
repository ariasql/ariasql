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
	INSERT INTO test (name) VALUES ('John Doe', 'Jane Doe');
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
