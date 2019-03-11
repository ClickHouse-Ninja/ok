package ok

import (
	"bytes"
	"encoding/csv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTSVtoArgs(t *testing.T) {
	var (
		body = bytes.NewBuffer([]byte{})
		tsv  = csv.NewWriter(body)
	)
	tsv.Comma = '\t'
	tsv.Write([]string{"1.1", "2.2", "1", "2", "3", "4", "10", "20", "30", "40", "Str", "00000000-0000-0000-0000-000000000000", "2019-02-09", "2019-02-09 10:10:10"})
	tsv.Write([]string{"10.10", "20.20", "10", "20", "30", "40", "100", "200", "300", "400", "Str 2", "00000000-0000-0000-0000-000000000000", "2019-02-09", "2019-02-09 10:10:10"})
	tsv.Flush()
	if rows, err := tsvToArgs([]string{
		"Float32",
		"Float64",
		"Int8",
		"Int16",
		"Int32",
		"Int64",
		"UInt8",
		"UInt16",
		"UInt32",
		"UInt64",
		"String",
		"UUID",
		"Date",
		"DateTime",
	}, body); assert.NoError(t, err) {
		if assert.Len(t, rows, 2) {
			{
				assert.Equal(t, float32(1.1), rows[0][0])
				assert.Equal(t, float64(2.2), rows[0][1])
			}
			{
				assert.Equal(t, int8(1), rows[0][2])
				assert.Equal(t, int16(2), rows[0][3])
				assert.Equal(t, int32(3), rows[0][4])
				assert.Equal(t, int64(4), rows[0][5])
			}
			{
				assert.Equal(t, uint8(10), rows[0][6])
				assert.Equal(t, uint16(20), rows[0][7])
				assert.Equal(t, uint32(30), rows[0][8])
				assert.Equal(t, uint64(40), rows[0][9])
			}
			if assert.Equal(t, "Str", rows[0][10]) {
				assert.Equal(t, "00000000-0000-0000-0000-000000000000", rows[0][11])
			}
			if tm, ok := rows[0][12].(time.Time); assert.True(t, ok) {
				assert.Equal(t, "2019-02-09", tm.Format("2006-01-02"))
			}
			if tm, ok := rows[0][13].(time.Time); assert.True(t, ok) {
				assert.Equal(t, "2019-02-09 10:10:10", tm.Format("2006-01-02 15:04:05"))
			}
		}
	}
}
