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
