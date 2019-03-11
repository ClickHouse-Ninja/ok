package ok

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func tsvToArgs(types []string, r io.Reader) (result [][]interface{}, err error) {
	reader := csv.NewReader(r)
	reader.Comma = '\t'
	for columns := []string{}; ; {
		if columns, err = reader.Read(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(types) != len(columns) {
			return nil, fmt.Errorf("expected %d columns got %d", len(types), len(columns))
		}
		var (
			value interface{}
			row   = make([]interface{}, 0, len(types))
		)
		for i, t := range types {
			converter, err := converterFactory(t)
			if err != nil {
				return nil, err
			}
			if value, err = converter(columns[i]); err != nil {
				return nil, err
			}
			row = append(row, value)
		}
		result = append(result, row)
	}
	return result, nil
}

type converter func(src string) (interface{}, error)

func converterFactory(t string) (converter, error) {
	switch t {
	case "String", "UUID":
		return func(src string) (interface{}, error) { return src, nil }, nil
	case "Date", "DateTime",
		"UInt8", "UInt16", "UInt32", "UInt64":
		return converters[t], nil
	default:
		if strings.HasPrefix(t, "Array") {

		}
	}
	return nil, fmt.Errorf("converter '%s' not found", t)
}

var converters = map[string]converter{
	"Date":     Date,
	"DateTime": DateTime,
	"Int8":     Int(8, func(v int64) interface{} { return int8(v) }),
	"Int16":    Int(16, func(v int64) interface{} { return int16(v) }),
	"Int32":    Int(32, func(v int64) interface{} { return int32(v) }),
	"Int64":    Int(64, func(v int64) interface{} { return int64(v) }),
	"UInt8":    UInt(8, func(v uint64) interface{} { return uint8(v) }),
	"UInt16":   UInt(16, func(v uint64) interface{} { return uint16(v) }),
	"UInt32":   UInt(32, func(v uint64) interface{} { return uint32(v) }),
	"UInt64":   UInt(64, func(v uint64) interface{} { return uint64(v) }),
}

func Date(str string) (interface{}, error) {
	value, err := time.Parse("2006-01-02", str)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func DateTime(str string) (interface{}, error) {
	value, err := time.Parse("2006-01-02 15:04:05", str)
	if err != nil {
		return nil, err
	}
	return value.Add(time.Nanosecond), nil
}

// UInt <T>
func UInt(bitSize int, cast func(uint64) interface{}) converter {
	return func(src string) (interface{}, error) {
		value, err := strconv.ParseUint(src, 10, bitSize)
		if err != nil {
			return 0, err
		}
		return cast(value), nil
	}
}

// Int <T>
func Int(bitSize int, cast func(int64) interface{}) converter {
	return func(src string) (interface{}, error) {
		value, err := strconv.ParseInt(src, 10, bitSize)
		if err != nil {
			return 0, err
		}
		return cast(value), nil
	}
}
