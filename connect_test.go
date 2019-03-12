package ok

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseQuery(t *testing.T) {
	if database, table, columns := parseQuery(`INSERT inTO database.table ( a, b,s ,x) Values ()`); assert.Equal(t, "database", database) {
		if assert.Equal(t, "table", table) {
			assert.Equal(t, []string{"a", "b", "s", "x"}, columns)
		}
	}

	if database, table, columns := parseQuery(`INSERT INTO db.table
		(
			a,
			b,
			s ,x)
		Values ()`); assert.Equal(t, "db", database) {
		if assert.Equal(t, "table", table) {
			assert.Equal(t, []string{"a", "b", "s", "x"}, columns)
		}
	}
}

func TestColumnTypes(t *testing.T) {
	conn := Connect(t, "tcp://127.0.0.1:9000?debug=0")
	if version, err := conn.Version(); assert.NoError(t, err) && !version.Less(&Version{18, 0, 0}) {
		if columnTypes, err := conn.(*clickhouse).columnTypes("system", "tables", []string{"database", "engine"}); assert.NoError(t, err) {
			assert.Equal(t, []string{"String", "String"}, columnTypes)
		}
	}
}

func TestVersion(t *testing.T) {
	if assert.True(t, (&Version{1, 2, 3}).Equal(&Version{1, 2, 3})) {

	}
	if assert.True(t, (&Version{1, 2, 3}).Less(&Version{1, 2, 4})) {
		assert.True(t, (&Version{1, 2, 3}).Less(&Version{2, 0, 0}))
		assert.True(t, (&Version{2, 0, 3}).Less(&Version{2, 1, 0}))
	}
	if assert.False(t, (&Version{2, 2, 3}).Less(&Version{1, 2, 4})) {
		assert.False(t, (&Version{2, 2, 3}).Less(&Version{2, 0, 0}))
		assert.False(t, (&Version{2, 1, 3}).Less(&Version{2, 1, 0}))
	}
	clickhouse := Connect(t, "tcp://127.0.0.1:9000?debug=0")

	if version, err := clickhouse.Version(); assert.NoError(t, err) {
		switch {
		case version.Less(&Version{18, 0, 0}):
			t.Logf("old version: %s", version)
		case version.Less(&Version{19, 0, 0}):
			t.Logf("version 18 X: %s", version)
		case version.Less(&Version{20, 0, 0}):
			t.Logf("version 19 X: %s", version)
		default:
			t.Logf("version: %s", version)
		}
	}
}

func TestBase(t *testing.T) {
	clickhouse := Connect(t, "tcp://127.0.0.1:9000?debug=0")
	if databases, err := clickhouse.ShowDatabases(); assert.NoError(t, err) {
		if assert.True(t, len(databases) != 0) {
			var exists bool
			for _, database := range databases {
				if database == "system" {
					exists = true
					break
				}
			}
			assert.True(t, exists)
		}
	}

	if tables, err := clickhouse.ShowTables("system"); assert.NoError(t, err) {
		if assert.True(t, len(tables) != 0) {
			var exists bool
			for _, table := range tables {
				if table == "settings" {
					exists = true
					break
				}
			}
			assert.True(t, exists)
		}
	}

	if exists := clickhouse.DatabaseExists("system"); assert.True(t, exists) {
		assert.False(t, clickhouse.DatabaseExists("not-exists"))
	}

	if exists := clickhouse.TableExists("system", "settings"); assert.True(t, exists) {
		assert.False(t, clickhouse.TableExists("system", "not-exists"))
	}

}

func TestDictionary(t *testing.T) {
	clickhouse := Connect(t, "tcp://127.0.0.1:9000?debug=0")
	if exists := clickhouse.DictionaryExists("dictionary"); exists {
		assert.True(t, clickhouse.ReloadDictionary("dictionary"))
	}
}

func TestExtractCreateDatabase(t *testing.T) {
	assets := map[string]string{
		"CREATE DATABASE test;":              "test",
		"CREATE DATABASE test":               "test",
		"CREATE DATABASE IF NOT EXISTS test": "test",
		"create \n database test":            "test",
		"CREATE TABLE db.table":              "",
	}
	for src, expected := range assets {
		assert.Equal(t, expected, extractCreateDatabase(src))
	}
}
func TestExtractCreateTable(t *testing.T) {
	assets := map[string][]string{
		"CREATE TABLE table":                  []string{"", "table"},
		"CREATE TABLE db.table":               []string{"db", "table"},
		"CREATE TABLE IF NOT exists db.table": []string{"db", "table"},
	}
	for src, expected := range assets {
		database, table := extractCreateTable(src)
		{
			assert.Equal(t, expected, []string{database, table})
		}
	}
}
