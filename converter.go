package ok

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func csvToArgs(types []string, r io.Reader, comma rune) (result [][]interface{}, err error) {
	reader := csv.NewReader(r)
	reader.Comma = comma
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
		"Float32", "Float64",
		"Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64":
		return converters[t], nil
	default:
		switch {
		case strings.HasPrefix(t, "Array"):
			base, err := converterFactory(t[6 : len(t)-1])
			if err != nil {
				return nil, err
			}
			return arrayT(base), nil
		case strings.HasPrefix(t, "Enum"):
			return func(src string) (interface{}, error) { return src, nil }, nil
		}
	}
	return nil, fmt.Errorf("converter '%s' not found", t)
}

var converters = map[string]converter{
	"Date":     date,
	"DateTime": dateTime,
	"Int8":     intT(8, func(v int64) interface{} { return int8(v) }),
	"Int16":    intT(16, func(v int64) interface{} { return int16(v) }),
	"Int32":    intT(32, func(v int64) interface{} { return int32(v) }),
	"Int64":    intT(64, func(v int64) interface{} { return int64(v) }),
	"UInt8":    uintT(8, func(v uint64) interface{} { return uint8(v) }),
	"UInt16":   uintT(16, func(v uint64) interface{} { return uint16(v) }),
	"UInt32":   uintT(32, func(v uint64) interface{} { return uint32(v) }),
	"UInt64":   uintT(64, func(v uint64) interface{} { return v }),
	"Float32":  floatT(32, func(v float64) interface{} { return float32(v) }),
	"Float64":  floatT(64, func(v float64) interface{} { return v }),
}

func date(str string) (interface{}, error) {
	value, err := time.Parse("2006-01-02", str)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func dateTime(str string) (interface{}, error) {
	value, err := time.Parse("2006-01-02 15:04:05", str)
	if err != nil {
		return nil, err
	}
	return value.Add(time.Nanosecond), nil
}

// Int <T>
func intT(bitSize int, cast func(int64) interface{}) converter {
	return func(src string) (interface{}, error) {
		value, err := strconv.ParseInt(src, 10, bitSize)
		if err != nil {
			return 0, err
		}
		return cast(value), nil
	}
}

// UInt <T>
func uintT(bitSize int, cast func(uint64) interface{}) converter {
	return func(src string) (interface{}, error) {
		value, err := strconv.ParseUint(src, 10, bitSize)
		if err != nil {
			return 0, err
		}
		return cast(value), nil
	}
}

// Float <T>
func floatT(bitSize int, cast func(float64) interface{}) converter {
	return func(src string) (interface{}, error) {
		value, err := strconv.ParseFloat(src, bitSize)
		if err != nil {
			return 0, err
		}
		return cast(value), nil
	}
}

// Array <T>
func arrayT(convert converter) converter {
	return func(src string) (v interface{}, err error) {
		var (
			slice  reflect.Value
			values = strings.Split(src[1:len(src)-1], ",")
		)
		for _, value := range values {
			if value[0] == '\'' && value[len(value)-1] == '\'' {
				value = value[1 : len(value)-1]
			}
			switch v, err = convert(value); {
			case err != nil:
				return nil, err
			case !slice.IsValid():
				var sliceType interface{}
				switch v.(type) {
				case int8:
					sliceType = []int8{}
				case int16:
					sliceType = []int16{}
				case int32:
					sliceType = []int32{}
				case int64:
					sliceType = []int64{}
				case uint8:
					sliceType = []uint8{}
				case uint16:
					sliceType = []uint16{}
				case uint32:
					sliceType = []uint32{}
				case uint64:
					sliceType = []uint64{}
				case float32:
					sliceType = []float32{}
				case float64:
					sliceType = []float64{}
				case string:
					sliceType = []string{}
				case time.Time:
					sliceType = []time.Time{}
				default:
					return nil, fmt.Errorf("unsupported Array type '%T'", v)
				}
				slice = reflect.MakeSlice(reflect.TypeOf(sliceType), 0, len(values))
			}
			slice = reflect.Append(slice, reflect.ValueOf(v))
		}
		return slice.Interface(), nil
	}
}
