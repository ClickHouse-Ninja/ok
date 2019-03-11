package ok

import (
	"testing"

	_ "github.com/kshvakov/clickhouse"
)

func TestParseQuery(t *testing.T) {
	t.Log(parseQuery(`INSERT inTO database.table ( a, b,s ,x) Values ()`))
	t.Log(parseQuery(`INSERT INTO table
		(
			a,
			b,
			s ,x)
		Values ()`))
}

func TestColumnTypes(t *testing.T) {
	conn := Connect(t, "tcp://127.0.0.1:9000?debug=0")

	t.Log(conn.(*clickhouse).columnTypes("system", "tables", []string{"database", "engine"}))

}
