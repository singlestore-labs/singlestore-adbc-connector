// Copyright (c) 2025 ADBC Drivers Contributors
// Copyright (c) 2026 SingleStore, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package singlestore

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-ext/variant"
	// register the "mysql" driver with database/sql
	_ "github.com/memsql/go-singlestore-driver"
)

// singlestoreTypeConverter provides SingleStore-specific type conversion enhancements
type singlestoreTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

// ConvertRawColumnType implements TypeConverter with SingleStore-specific enhancements
func (m *singlestoreTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	switch typeName {
	case "BIT":
		// Handle BIT type as binary data
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			"singlestore.is_enum_set":          "true",
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "TIMESTAMP":
		var timestampType arrow.DataType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			if precision > 6 {
				precision = 6
			}
			timeUnit := arrow.TimeUnit(precision / 3)
			timestampType = &arrow.TimestampType{Unit: timeUnit, TimeZone: "UTC"}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil

	case "YEAR":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.PrimitiveTypes.Int16, nullable, metadata, nil

	case "FLOAT":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.PrimitiveTypes.Float32, nullable, metadata, nil

	case "DOUBLE":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.PrimitiveTypes.Float64, nullable, metadata, nil

	case "DECIMAL", "NUMERIC":
		if colType.Precision != nil && colType.Scale != nil {
			precision := *colType.Precision
			scale := *colType.Scale

			arrowType, err := arrow.NarrowestDecimalType(int32(precision), int32(scale))
			if err != nil {
				return nil, false, arrow.Metadata{}, fmt.Errorf("invalid decimal precision/scale (%d, %d): %w", precision, scale, err)
			}

			// Build metadata with decimal information
			metadata := arrow.MetadataFrom(map[string]string{
				sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
				sqlwrapper.MetaKeyColumnName:       colType.Name,
				sqlwrapper.MetaKeyPrecision:        fmt.Sprintf("%d", precision),
				sqlwrapper.MetaKeyScale:            fmt.Sprintf("%d", scale),
			})

			return arrowType, nullable, metadata, nil
		}

		return m.DefaultTypeConverter.ConvertRawColumnType(colType)

	case "LONGBLOB":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.BinaryTypes.LargeBinary, nullable, metadata, nil

	case "LONGTEXT":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.BinaryTypes.LargeString, nullable, metadata, nil
	case "JSON":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.LargeString)
		if err != nil {
			return nil, false, arrow.Metadata{}, fmt.Errorf("error creating JSON type: %w", err)
		}

		return jsonType, nullable, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)
	}
}

// CreateInserter creates SingleStore-specific inserters bound to builders for enhanced performance
func (m *singlestoreTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for SingleStore-specific types first
	switch field.Type.(type) {
	case *extensions.JSONType:
		return &singlestoreJSONInserter{builder: builder}, nil
	case *arrow.BinaryType:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			return &singlestoreBitInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}

		// Fall through to default for binary
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	// Time types
	case *arrow.Time32Type:
		return &singlestoreTime32Inserter{builder: builder.(*array.Time32Builder)}, nil
	case *arrow.Time64Type:
		return &singlestoreTime64Inserter{builder: builder.(*array.Time64Builder)}, nil

	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// SingleStore-specific inserters
type singlestoreTime64Inserter struct {
	builder *array.Time64Builder
}

func (ins *singlestoreTime64Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	timeType := ins.builder.Type().(*arrow.Time64Type)
	if timeType.Unit != arrow.Microsecond {
		return fmt.Errorf("unsupported Time64 unit: %v", timeType.Unit)
	}

	val, err := convertToTime64(unwrapped)
	if err != nil {
		return err
	}
	ins.builder.Append(val)
	return nil
}

// convertToTime64 converts a SQL value to arrow.Time64 type
func convertToTime64(val any) (arrow.Time64, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Time64(int64(v.Hour())*3600000000 + int64(v.Minute()*60000000) + int64(v.Second()*1000000) + int64(v.Nanosecond())/1000), nil
	case []byte:
		return time64FromString(string(v))
	case string:
		return time64FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time64, expected time.Time", val)
	}
}

func time64FromString(timeStr string) (arrow.Time64, error) {
	isNegative := false

	// Handle the negative sign
	if strings.HasPrefix(timeStr, "-") {
		isNegative = true
		timeStr = strings.TrimPrefix(timeStr, "-")
	}

	// Split into HH, MM, and SS.FFFFFF
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: expected HH:MM:SS[.frac], got %s", timeStr)
	}

	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing hours: %w", err)
	}

	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing minutes: %w", err)
	}

	// Separate seconds from fractional microseconds
	secParts := strings.Split(parts[2], ".")
	seconds, err := strconv.ParseInt(secParts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing seconds: %w", err)
	}

	var microseconds int64 = 0

	// Handle the fractional part if it exists
	if len(secParts) > 1 {
		fractionStr := secParts[1]

		// SingleStore supports up to 6 digits (microseconds).
		// We must pad the string with zeros on the right so ".7" becomes "700000".
		if len(fractionStr) > 6 {
			fractionStr = fractionStr[:6] // Truncate to 6 digits if it's too long
		} else if len(fractionStr) < 6 {
			fractionStr = fractionStr + strings.Repeat("0", 6-len(fractionStr))
		}

		microseconds, err = strconv.ParseInt(fractionStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing microseconds: %w", err)
		}
	}

	// Calculate total microseconds
	totalMicroseconds := (hours * 3600000000) +
		(minutes * 60000000) +
		(seconds * 1000000) +
		microseconds

	// Reapply negative sign
	if isNegative {
		totalMicroseconds = -totalMicroseconds
	}

	return arrow.Time64(totalMicroseconds), nil
}

type singlestoreTime32Inserter struct {
	builder *array.Time32Builder
}

func (ins *singlestoreTime32Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	timeType := ins.builder.Type().(*arrow.Time32Type)
	if timeType.Unit != arrow.Second {
		return fmt.Errorf("unsupported Time32 unit: %v", timeType.Unit)
	}

	val, err := convertToTime32(unwrapped)
	if err != nil {
		return err
	}
	ins.builder.Append(val)
	return nil
}

func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// convertToTime32 converts a SQL value to arrow.Time32 type
func convertToTime32(val any) (arrow.Time32, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Time32(v.Hour()*3600 + v.Minute()*60 + v.Second()), nil
	case []byte:
		return time32FromString(string(v))
	case string:
		return time32FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time32, expected time.Time", val)
	}
}

func time32FromString(timeStr string) (arrow.Time32, error) {
	isNegative := false

	// Check for and remove the negative sign
	if strings.HasPrefix(timeStr, "-") {
		isNegative = true
		timeStr = strings.TrimPrefix(timeStr, "-")
	}

	// Split the string into Hours, Minutes, and Seconds
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: expected HH:MM:SS, got %s", timeStr)
	}

	// Parse each part into an int64
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing hours: %w", err)
	}

	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing minutes: %w", err)
	}

	seconds, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing seconds: %w", err)
	}

	// Calculate total seconds (Hours * 3600 + Minutes * 60 + Seconds)
	totalSeconds := (hours * 3600) + (minutes * 60) + seconds

	// Reapply the negative sign if the original string was negative
	if isNegative {
		totalSeconds = -totalSeconds
	}

	return arrow.Time32(totalSeconds), nil
}

type singlestoreJSONInserter struct {
	builder array.Builder
}

func (ins *singlestoreJSONInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for singlestore json inserter, got %T", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(string(t))
}

type singlestoreBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *singlestoreBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for singlestore bit inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

// ConvertArrowToGo implements SingleStore-specific Arrow value to Go value conversion
func (m *singlestoreTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle SingleStore-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *extensions.JSONArray:
		// Handle JSON extension type arrays
		jsonStr := a.ValueStr(index)
		v := variant.New(jsonStr)
		return v, nil

	case *array.Time32:
		// For SingleStore driver, always convert Time32 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Time64:
		// For SingleStore driver, always convert Time64 arrays to time-only format strings
		// This handles both explicit TIME column metadata and parameter binding scenarios
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000"), nil

	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		rawValue := a.Value(index)
		t := rawValue.ToTime(timestampType.Unit)

		// For nanosecond precision, truncate to microseconds
		if timestampType.Unit == arrow.Nanosecond {
			microseconds := t.UnixMicro()
			converted := time.UnixMicro(microseconds).UTC()
			return converted, nil
		}

		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// singlestoreConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type singlestoreConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version string
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*singlestoreConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*singlestoreConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*singlestoreConnectionImpl)(nil)

// implements TableTypeLister interface
var _ driverbase.TableTypeLister = (*singlestoreConnectionImpl)(nil)

// singlestoreConnectionFactory creates SingleStore connections
type singlestoreConnectionFactory struct{}

// CreateConnection implements sqlwrapper.ConnectionFactory
func (f *singlestoreConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with SingleStore-specific functionality
	return &singlestoreConnectionImpl{
		ConnectionImplBase: conn,
	}, nil
}

// NewDriver constructs the ADBC Driver for "singlestore".
func NewDriver(alloc memory.Allocator) adbc.Driver {
	vendorName := "SingleStore"
	typeConverter := &singlestoreTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: vendorName},
	}

	driver := sqlwrapper.NewDriver(alloc, "mysql", vendorName, NewSingleStoreDBFactory(), typeConverter).
		WithConnectionFactory(&singlestoreConnectionFactory{}).
		WithErrorInspector(SingleStoreErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver for SingleStore",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
