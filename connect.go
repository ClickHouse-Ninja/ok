package ok

import (
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
)

type ClickHouse interface {
	DB() *sql.DB
	Exec(query string) error
	Version() (*Version, error)
	ShowDatabases() ([]string, error)
	ShowTables(from ...string) ([]string, error)
	DatabaseExists(database string) bool
	TableExists(database, table string) bool
	DictionaryExists(dictionary string) bool
	ReloadDictionary(dictionary string) bool
	CopyFromCSVReader(r io.Reader, sql string) bool
	CopyFromTSVReader(r io.Reader, sql string) bool
	CopyFromCSVFile(path, sql string) bool
	CopyFromTSVFile(path, sql string) bool
	DropDatabase(database string) bool
	DropTable(database, table string) bool
	Clear() bool
}

type Version struct {
	Major int
	Minor int
	Patch int
}

func (v *Version) Less(v2 *Version) bool {
	var (
		majorEq = v.Major == v2.Major
		minorEq = majorEq && v.Minor == v2.Minor
	)
	return v.Major < v2.Major || (majorEq && v.Minor < v2.Minor) || (minorEq && v.Patch < v2.Patch)
}

func (v *Version) Equal(v2 *Version) bool {
	return v.Major == v2.Major && v.Minor == v2.Minor && v.Patch == v2.Patch
}

func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

func Connect(test *testing.T, dsn string) ClickHouse {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		test.Fatalf("could not open ClickHouse driver: %v", err)
	}
	conn.SetMaxOpenConns(1)
	var (
		url, _   = url.Parse(dsn)
		database = "default"
	)
	if value := url.Query().Get("database"); len(value) != 0 {
		database = value
	}
	return &clickhouse{
		test:     test,
		conn:     conn,
		database: database,
	}
}

type clickhouse struct {
	test     *testing.T
	conn     *sql.DB
	database string
	clear    struct {
		databases []string
		tables    [][]string
	}
}

func (c *clickhouse) DB() *sql.DB {
	return c.conn
}

func (c *clickhouse) Version() (*Version, error) {
	var version Version
	const query = `
		WITH (splitByChar('.',version())) AS version
		SELECT
			toInt16(version[1])   AS major
			, toInt16(version[2]) AS minor
			, toInt16(version[3]) AS patch
	`
	if err := c.conn.QueryRow(query).Scan(&version.Major, &version.Minor, &version.Patch); err != nil {
		return nil, err
	}
	return &version, nil
}

func (c *clickhouse) ShowDatabases() (databases []string, _ error) {
	rows, err := c.conn.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, err
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (c *clickhouse) ShowTables(from ...string) (tables []string, _ error) {
	query := "SHOW TABLES"
	if len(from) != 0 {
		query = "SHOW TABLES FROM " + from[0]
	}
	rows, err := c.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (c *clickhouse) DatabaseExists(database string) bool {
	exists, err := c.exists("SELECT COUNT() FROM system.databases WHERE name = ?", database)
	if err != nil {
		c.test.Errorf("an error occurred while checking the database: %v", err)
		return false
	}
	return exists
}

func (c *clickhouse) TableExists(database, table string) bool {
	exists, err := c.exists("SELECT COUNT() FROM system.tables WHERE database = ? AND name = ?", database, table)
	if err != nil {
		c.test.Errorf("an error occurred while checking the table: %v", err)
		return false
	}
	return exists
}

func (c *clickhouse) DictionaryExists(dictionary string) bool {
	exists, err := c.exists("SELECT COUNT() FROM system.dictionaries WHERE name = ?", dictionary)
	if err != nil {
		c.test.Errorf("an error occurred while checking the dictionary: %v", err)
		return false
	}
	return exists
}

func (c *clickhouse) exists(query string, args ...interface{}) (bool, error) {
	var count int
	if err := c.conn.QueryRow(query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (c *clickhouse) ReloadDictionary(dictionary string) bool {
	if _, err := c.conn.Exec("SYSTEM RELOAD DICTIONARY " + quote(dictionary)); err != nil {
		c.test.Errorf("an error occurred while reloading dictionary: %v", err)
		return false
	}
	return true
}

func (c *clickhouse) DropDatabase(database string) bool {
	if err := c.Exec("DROP DATABASE IF EXISTS " + database); err != nil {
		c.test.Errorf("an error occurred while deleting the database: %v", err)
		return false
	}
	return true
}

func (c *clickhouse) DropTable(database, table string) bool {
	if err := c.Exec("DROP TABLE IF EXISTS " + database + "." + table); err != nil {
		c.test.Errorf("an error occurred while deleting the table: %v", err)
		return false
	}
	return true
}

func (c *clickhouse) Exec(query string) error {
	for _, query := range strings.Split(query, ";\n") {
		if query = strings.TrimSpace(query); len(query) != 0 {
			if _, err := c.conn.Exec(query); err != nil {
				return err
			}
			if database := extractCreateDatabase(query); len(database) != 0 {
				c.clear.databases = append(c.clear.databases, database)
			}
			if database, table := extractCreateTable(query); len(table) != 0 {
				c.clear.tables = append(c.clear.tables, []string{database, table})
			}
		}
	}
	return nil
}

func (c *clickhouse) CopyFromCSVReader(r io.Reader, query string) bool {
	return c.copyFromReader(r, query, ',')
}

func (c *clickhouse) CopyFromTSVReader(r io.Reader, query string) bool {
	return c.copyFromReader(r, query, '\t')
}

func (c *clickhouse) CopyFromCSVFile(path, query string) bool {
	return c.copyFromFile(path, query, ',')
}

func (c *clickhouse) CopyFromTSVFile(path, query string) bool {
	return c.copyFromFile(path, query, '\t')
}

func (c *clickhouse) copyFromFile(path, query string, comma rune) bool {
	file, err := os.Open(path)
	if err != nil {
		c.test.Errorf("could not open file: %v", err)
		return false
	}
	defer file.Close()
	return c.copyFromReader(file, query, comma)
}

func (c *clickhouse) copyFromReader(r io.Reader, query string, comma rune) bool {
	database, table, columns := parseQuery(query)
	if len(table) == 0 {
		c.test.Error("error while parsing query: cannot find table name")
		return false
	}
	if len(database) == 0 {
		database = c.database
	}
	columnTypes, err := c.columnTypes(database, table, columns)
	if err != nil {
		c.test.Error(err)
		return false
	}
	rows, err := csvToArgs(columnTypes, r, comma)
	if err != nil {
		c.test.Error(err)
		return false
	}
	scope, err := c.conn.Begin()
	if err != nil {
		c.test.Error(err)
		return false
	}
	block, err := scope.Prepare(query)
	if err != nil {
		c.test.Error(err)
		return false
	}
	for _, row := range rows {
		if _, err := block.Exec(row...); err != nil {
			scope.Rollback()
			{
				c.test.Error(err)
			}
			return false
		}
	}
	if err := scope.Commit(); err != nil {
		c.test.Error(err)
		return false
	}
	return true
}

func (c *clickhouse) Clear() bool {
	ok := true
	for _, tuple := range c.clear.tables {
		database := c.database
		if len(tuple[0]) != 0 {
			database = tuple[0]
		}
		if !c.DropTable(database, tuple[1]) {
			ok = false
		}
	}
	for _, database := range c.clear.databases {
		if !c.DropDatabase(database) {
			ok = false
		}
	}
	return ok
}

func (c *clickhouse) columnTypes(database, table string, columns []string) (types []string, err error) {
	var (
		args  = []interface{}{database, table}
		query = "SELECT name, type FROM system.columns WHERE database = ? AND table = ?"
	)
	if len(columns) != 0 {
		query += " AND name IN(?)"
		args = append(args, columns)
	}
	rows, err := c.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	columnTypes := make(map[string]string, len(columns))
	for rows.Next() {
		var (
			column, columnType string
		)
		if err := rows.Scan(&column, &columnType); err != nil {
			return nil, err
		}
		switch {
		case len(columns) == 0:
			types = append(types, columnType)
		default:
			columnTypes[column] = columnType
		}
	}
	for _, column := range columns {
		if _, found := columnTypes[column]; !found {
			return nil, fmt.Errorf("column '%s' does not exists", column)
		}
		types = append(types, columnTypes[column])
	}
	return types, nil
}

var _ ClickHouse = (*clickhouse)(nil)

func quote(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

func parseQuery(query string) (database string, table string, columns []string) {
	var (
		isOpen       bool
		fields       = strings.Fields(query)
		appendColumn = func(field string) {
			for _, column := range strings.Split(field, ",") {
				if len(column) != 0 {
					columns = append(columns, column)
				}
			}
		}
	)
parse:
	for i, field := range fields {
		if len(table) == 0 && strings.ToUpper(field) == "INTO" {
			switch parts := strings.Split(fields[i+1], "."); len(parts) {
			case 1:
				table = parts[0]
			case 2:
				database, table = parts[0], parts[1]
			}
		}
		if len(field) == 0 || field == "," {
			continue parse
		}
		switch {
		case field == "(" || strings.HasPrefix(field, "("):
			switch {
			case strings.HasSuffix(field, ")"):
				appendColumn(field[1 : len(field)-1])
				break parse
			default:
				appendColumn(field[1:])
			}
			isOpen = true
		case field == ")" || strings.HasSuffix(field, ")"):
			appendColumn(field[:len(field)-1])
			break parse
		case isOpen:
			appendColumn(field)
		}
	}

	return database, table, columns
}

func extractCreateDatabase(ddl string) string {
	var check bool
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddl)), "CREATE") {
		for _, field := range strings.Fields(ddl) {
			switch {
			case strings.ToUpper(field) == "DATABASE" && !check:
				check = true
			case check:
				switch strings.ToUpper(field) {
				case "IF", "NOT", "EXISTS":
				default:
					if len(field) != 0 {
						return strings.TrimSpace(strings.TrimSuffix(field, ";"))
					}
				}
			}
		}
	}
	return ""
}

func extractCreateTable(ddl string) (string, string) {
	var check bool
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ddl)), "CREATE") {
		for _, field := range strings.Fields(ddl) {
			switch {
			case strings.ToUpper(field) == "TABLE" && !check:
				check = true
			case check:
				if field = strings.TrimSpace(strings.TrimSuffix(field, "(")); len(field) != 0 {
					switch strings.ToUpper(field) {
					case "IF", "NOT", "EXISTS":
					default:
						if parts := strings.Split(field, "."); len(parts) == 2 {
							return parts[0], parts[1]
						}
						return "", field
					}
				}
			}
		}
	}
	return "", ""
}
