# ClickHouse testing suite [![Build Status](https://travis-ci.org/ClickHouse-Ninja/ok.svg?branch=master)](https://travis-ci.org/ClickHouse-Ninja/ok) [![codecov](https://codecov.io/gh/ClickHouse-Ninja/ok/branch/master/graph/badge.svg)](https://codecov.io/gh/ClickHouse-Ninja/ok) [![GoDoc](https://godoc.org/github.com/ClickHouse-Ninja/ok?status.svg)](https://godoc.org/github.com/ClickHouse-Ninja/ok)

This is a small framework to help test Go and ClickHouse applications.

Example:

```go
package ok_test

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"testing"

	"github.com/ClickHouse-Ninja/ok"
	_ "github.com/kshvakov/clickhouse"
	"github.com/stretchr/testify/assert"
)

type App struct {
	conn *sql.DB
}

func (app *App) Count() (count int, err error) {
	if err = app.conn.QueryRow("SELECT COUNT() FROM tester.table").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func TestExapmple(t *testing.T) {
	ok := ok.Connect(t, "tcp://127.0.0.1:9000?debug=0")
	if ok.DatabaseExists("tester") {
		t.Fatal("database 'tester' is already exists")
	}
	defer ok.Clear()
	const ddl = `
	CREATE DATABASE tester;
	CREATE TABLE tester.table (
		event_time   DateTime
		, event_type String
		, user_id    UInt64
		, value      UInt32
	) Engine Memory;
	`
	if err := ok.Exec(ddl); err != nil {
		t.Fatalf("an error occurred while creating the test table: %v", err)
	}
	var (
		buf    bytes.Buffer
		writer = csv.NewWriter(&buf)
	)
	writer.Comma = '\t'
	writer.WriteAll([][]string{
		[]string{"2019-03-08 21:00:00", "view", "1", "2"},
		[]string{"2019-03-08 21:00:01", "click", "1", "2"},
	})
	writer.Flush()
	if ok.CopyFromTSVReader(&buf, "INSERT INTO tester.table (event_time, event_type, user_id, value) VALUES") {
		app := App{
			conn: ok.DB(),
		}
		if count, err := app.Count(); assert.NoError(t, err) {
			assert.Equal(t, 2, count)
		}
	}
}
```