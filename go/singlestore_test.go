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

package singlestore_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/singlestore-labs/singlestore-adbc-connector"
)

// SingleStoreQuirks implements validation.DriverQuirks for SingleStore ADBC driver
type SingleStoreQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *SingleStoreQuirks) QuoteTableName(tableName string) string {
	return "`" + strings.ReplaceAll(tableName, "`", "``") + "`"
}

func (q *SingleStoreQuirks) SetupDriver(t *testing.T) adbc.Driver {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return singlestore.NewDriver(q.mem)
}

func (q *SingleStoreQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	q.mem.AssertSize(t, 0)
}

func (q *SingleStoreQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *SingleStoreQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("mysql", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to SingleStore types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}

		if !field.Nullable {
			createQuery.WriteString(" NOT NULL")
		}
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]interface{}, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []interface{}
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					insertQuery.WriteString("?")
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *SingleStoreQuirks) DropTable(cnxn adbc.Connection, tblName string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	if err = stmt.SetSqlQuery("DROP TABLE IF EXISTS " + tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *SingleStoreQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our SingleStore type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "int"
		metadata["sql.precision"] = "10"
		metadata["sql.scale"] = "0"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "bigint"
		metadata["sql.precision"] = "19"
		metadata["sql.scale"] = "0"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "varchar"
		metadata["sql.length"] = "255"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "float"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "double"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "tinyint"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *SingleStoreQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *SingleStoreQuirks) BindParameter(idx int) string { return "?" }

func (q *SingleStoreQuirks) SupportsBulkIngest(string) bool        { return true }
func (q *SingleStoreQuirks) SupportsConcurrentStatements() bool    { return false }
func (q *SingleStoreQuirks) SupportsCurrentCatalogSchema() bool    { return true }
func (q *SingleStoreQuirks) SupportsExecuteSchema() bool           { return true }
func (q *SingleStoreQuirks) SupportsGetSetOptions() bool           { return true }
func (q *SingleStoreQuirks) SupportsPartitionedData() bool         { return false }
func (q *SingleStoreQuirks) SupportsStatistics() bool              { return false }
func (q *SingleStoreQuirks) SupportsTransactions() bool            { return false }
func (q *SingleStoreQuirks) SupportsGetParameterSchema() bool      { return false }
func (q *SingleStoreQuirks) SupportsDynamicParameterBinding() bool { return true }

// TODO: enable when error check is fixed
// https://github.com/adbc-drivers/driverbase-go/pull/123
func (q *SingleStoreQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *SingleStoreQuirks) Catalog() string                             { return "db" }
func (q *SingleStoreQuirks) DBSchema() string                            { return "" }

func (q *SingleStoreQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver for SingleStore"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return os.Getenv("SINGLESTORE_VERSION") + " (SingleStoreDB source distribution (compatible; MySQL Enterprise & MySQL Commercial))"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "SingleStore"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*SingleStoreQuirks)) {
	dsn := os.Getenv("SINGLESTORE_DSN")
	if dsn == "" {
		t.Skip("Set SINGLESTORE_DSN environment variable for validation tests")
	}

	q := &SingleStoreQuirks{dsn: dsn}
	fn(q)
}

type SingleStoreStatementTests struct {
	validation.StatementTests
}

func (s *SingleStoreStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *SingleStoreQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type SingleStoreTests struct {
	suite.Suite

	Quirks *SingleStoreQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *SingleStoreTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *SingleStoreTests) TearDownTest() {
	s.NoError(s.stmt.Close())
	s.NoError(s.cnxn.Close())
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close())
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *SingleStoreTests) TestSelect() {
	// Create test table with various SingleStore types including spatial
	s.NoError(s.stmt.SetSqlQuery(`
		CREATE ROWSTORE TEMPORARY TABLE test_types (
		  id int PRIMARY KEY,
		  bool_col BOOL,
		  boolean_col BOOLEAN,
		  bit_col BIT(64),
		  tinyint_col TINYINT,
		  mediumint_col MEDIUMINT,
		  smallint_col SMALLINT,
		  int_col INT,
		  integer_col INTEGER,
		  bigint_col BIGINT,
		  float_col FLOAT,
		  double_col DOUBLE,
		  real_col REAL,
		  date_col DATE,
		  time_col TIME,
		  time6_col TIME(6),
		  datetime_col DATETIME,
		  datetime6_col DATETIME(6),
		  timestamp_col TIMESTAMP,
		  timestamp6_col TIMESTAMP(6),
		  year_col YEAR,
		  decimal_col1 DECIMAL(65, 30),
		  decimal_col2 DECIMAL(38, 10),
		  decimal_col3 DECIMAL(18, 10),
		  decimal_col4 DECIMAL(9, 5),
		  decimal_col5 DECIMAL(18, 0),
		  dec_col DEC(20, 0),
		  fixed_col FIXED(20, 0),
		  numeric_col NUMERIC(20, 0),
		  char_col CHAR,
		  mediumtext_col MEDIUMTEXT,
		  binary_col BINARY,
		  varchar_col VARCHAR(100),
		  varbinary_col VARBINARY(100),
		  longtext_col LONGTEXT,
		  text_col TEXT,
		  tinytext_col TINYTEXT,
		  longblob_col LONGBLOB,
		  mediumblob_col MEDIUMBLOB,
		  blob_col BLOB,
		  tinyblob_col TINYBLOB,
		  json_col JSON,
		  bson_col BSON,
		  enum_col ENUM('active', 'inactive'),
		  set_col SET('a', 'b', 'c'),
		  geography_col GEOGRAPHY,
		  geographypoint_col GEOGRAPHYPOINT,
		  vector_i8_col VECTOR(2, I8),
		  vector_i16_col VECTOR(2, I16),
		  vector_i32_col VECTOR(2, I32),
		  vector_i64_col VECTOR(2, I64),
		  vector_f32_col VECTOR(2, F32),
		  vector_f64_col VECTOR(2, F64)
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data including spatial data
	s.NoError(s.stmt.SetSqlQuery(`
		INSERT INTO test_types VALUES (
		  1,                          -- id
		  0,                          -- bool_col
		  0,                          -- boolean_col
		  0,                          -- bit_col
		  -128,                       -- tinyint_col
		  -8388608,                   -- mediumint_col
		  -32768,                     -- smallint_col
		  -2147483648,                -- int_col
		  -2147483648,                -- integer_col
		  -9223372036854775808,       -- bigint_col
		  -3.402E+38,           -- float_col
		  -1.7976931348623157E+308,   -- double_col
		  -1.7976931348623157E+308,   -- real_col
		  '1000-01-01',               -- date_col
		  '-838:59:59',               -- time_col
		  '-838:59:59.000000',        -- time6_col
		  '1000-01-01 00:00:00',      -- datetime_col
		  '1000-01-01 00:00:00.000000', -- datetime6_col
		  '1970-01-01 00:00:01',      -- timestamp_col
		  '1970-01-01 00:00:01.000000', -- timestamp6_col
		  1901,                       -- year_col
		  '-99999999999999999999999999999999999.999999999999999999999999999999', -- decimal_col1
		  '-9999999999999999999999999999.9999999999', -- decimal_col2
		  '-99999999.9999999999',     -- decimal_col3
		  '-9999.99999',              -- decimal_col4
		  '-999999999999999999',      -- decimal_col5
		  -9999999999,                -- dec_col
		  -9999999999,                -- fixed_col
		  -9999999999,                -- numeric_col
		  '',                         -- char_col
		  '',                         -- mediumtext_col
		  0x00,                       -- binary_col
		  '',                         -- varchar_col
		  0x00,                       -- varbinary_col
		  '',                         -- longtext_col
		  '',                         -- text_col
		  '',                         -- tinytext_col
		  0x00,                       -- longblob_col
		  0x00,                       -- mediumblob_col
		  0x00,                       -- blob_col
		  0x00,                       -- tinyblob_col
		  '{}',                       -- json_col
		  '{}',                       -- bson_col
		  'active',                   -- enum_col
		  '',                         -- set_col
		  NULL,                       -- geography_col
		  NULL,                       -- geographypoint_col
		  '[-128, -128]',         -- vector_i8_col
		  '[-32768, -32768]',     -- vector_i16_col
		  '[-2147483648, -2147483648]', -- vector_i32_col
		  '[-9223372036854775808, -9223372036854775808]', -- vector_i64_col
		  '[-3.402823466E+38, -3.402823466E+38]', -- vector_f32_col
		  '[-1.7976931348623157E+308, -1.7976931348623157E+308]' -- vector_f64_col
		),
		(
		  2,                          -- id
		  1,                          -- bool_col
		  1,                          -- boolean_col
		  18446744073709551615,       -- bit_col
		  127,                        -- tinyint_col
		  8388607,                    -- mediumint_col
		  32767,                      -- smallint_col
		  2147483647,                 -- int_col
		  2147483647,                 -- integer_col
		  9000000000000000000,        -- bigint_col
		  3.402E+38,            -- float_col
		  1.7976931348623157E+308,    -- double_col
		  1.7976931348623157E+308,    -- real_col
		  '9999-12-31',               -- date_col
		  '838:59:59',                -- time_col
		  '838:59:58.999999',         -- time6_col
		  '9999-12-31 23:59:59',      -- datetime_col
		  '9999-12-31 23:59:59.999999', -- datetime6_col
		  '2038-01-19 03:14:07',      -- timestamp_col
		  '2038-01-19 03:14:06.999999', -- timestamp6_col
		  2155,                       -- year_col
		  '99999999999999999999999999999999999.999999999999999999999999999999', -- decimal_col1
		  '9999999999999999999999999999.9999999999', -- decimal_col2
		  '99999999.9999999999',      -- decimal_col3
		  '9999.99999',               -- decimal_col4
		  '999999999999999999',       -- decimal_col5
		  9999999999,                 -- dec_col
		  9999999999,                 -- fixed_col
		  9999999999,                 -- numeric_col
		  'Z',                        -- char_col (single character)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- mediumtext_col (example long string)
		  0xFF,                       -- binary_col (max byte value)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- varchar_col (100 chars)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- varbinary_col (100 bytes)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- longtext_col (example string)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- text_col (example string)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- tinytext_col (example string)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- longblob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- mediumblob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- blob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- tinyblob_col (example binary)
		  '{"key":"value"}',          -- json_col
		  '{"key":"value"}',          -- bson_col
		  'inactive',                 -- enum_col
		  'a,b,c',                    -- set_col
		  'POLYGON((0 0, 0 1, 1 1, 0 0))', -- geographypoint_col
		  'POINT(-74.044514 40.689244)', -- geography_col
		  '[127, 127]',           -- vector_i8_col
		  '[32767, 32767]',       -- vector_i16_col
		  '[2147483647, 2147483647]', -- vector_i32_col
		  '[9223372036854775807, 9223372036854775807]', -- vector_i64_col
		  '[3.402823466E+38, 3.402823466E+38]', -- vector_f32_col
		  '[1.7976931348623157E+308, 1.7976931348623157E+308]' -- vector_f64_col
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "bool",
			query: "SELECT bool_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bool_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bool_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"bool_col": 0}, {"bool_col": 1}]`,
		},
		{
			name:  "boolean",
			query: "SELECT boolean_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "boolean_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "boolean_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"boolean_col": 0}, {"boolean_col": 1}]`,
		},
		{
			name:  "bit",
			query: "SELECT bit_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bit_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bit_col",
						"sql.database_type_name": "BIT",
					}),
				},
			}, nil),
			expected: `[{"bit_col": "AAAAAAAAAAA="}, {"bit_col": "//////////8="}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "tinyint_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "tinyint_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"tinyint_col": -128}, {"tinyint_col": 127}]`,
		},
		{
			name:  "mediumint",
			query: "SELECT mediumint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "mediumint_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "mediumint_col",
						"sql.database_type_name": "MEDIUMINT",
					}),
				},
			}, nil),
			expected: `[{"mediumint_col": -8388608}, {"mediumint_col": 8388607}]`,
		},
		{
			name:  "smallint",
			query: "SELECT smallint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "smallint_col",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "smallint_col",
						"sql.database_type_name": "SMALLINT",
					}),
				},
			}, nil),
			expected: `[{"smallint_col": -32768}, {"smallint_col": 32767}]`,
		},
		{
			name:  "int",
			query: "SELECT int_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "int_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "int_col",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"int_col": -2147483648}, {"int_col": 2147483647}]`,
		},
		{
			name:  "integer",
			query: "SELECT integer_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "integer_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "integer_col",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"integer_col": -2147483648}, {"integer_col": 2147483647}]`,
		},
		{
			name:  "bigint",
			query: "SELECT bigint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bigint_col",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bigint_col",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"bigint_col": -9223372036854775808}, {"bigint_col": 9000000000000000000}]`,
		},
		{
			name:  "float",
			query: "SELECT float_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "float_col",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "float_col",
						"sql.database_type_name": "FLOAT",
					}),
				},
			}, nil),
			expected: `[{"float_col": -3.402e+38}, {"float_col": 3.402e+38}]`,
		},
		{
			name:  "double",
			query: "SELECT double_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "double_col",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "double_col",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"double_col": -1.7976931348623157e+308}, {"double_col": 1.7976931348623157e+308}]`,
		},
		{
			name:  "real",
			query: "SELECT real_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "real_col",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "real_col",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"real_col": -1.7976931348623157e+308}, {"real_col": 1.7976931348623157e+308}]`,
		},
		{
			name:  "decimal1",
			query: "SELECT decimal_col1 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col1",
					Type:     &arrow.Decimal256Type{Precision: 65, Scale: 30},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col1",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "65",
						"sql.scale":              "30",
					}),
				},
			}, nil),
			expected: `[{"decimal_col1": "-99999999999999999999999999999999999.999999999999999999999999999999"}, {"decimal_col1": "99999999999999999999999999999999999.999999999999999999999999999999"}]`,
		},
		{
			name:  "decimal2",
			query: "SELECT decimal_col2 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col2",
					Type:     &arrow.Decimal128Type{Precision: 38, Scale: 10},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col2",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "38",
						"sql.scale":              "10",
					}),
				},
			}, nil),
			expected: `[{"decimal_col2": "-9999999999999999999999999999.9999999999"}, {"decimal_col2": "9999999999999999999999999999.9999999999"}]`,
		},
		{
			name:  "decimal3",
			query: "SELECT decimal_col3 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col3",
					Type:     &arrow.Decimal64Type{Precision: 18, Scale: 10},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col3",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "18",
						"sql.scale":              "10",
					}),
				},
			}, nil),
			expected: `[{"decimal_col3": "-99999999.9999999999"}, {"decimal_col3": "99999999.9999999999"}]`,
		},
		{
			name:  "decimal4",
			query: "SELECT decimal_col4 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col4",
					Type:     &arrow.Decimal32Type{Precision: 9, Scale: 5},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col4",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "9",
						"sql.scale":              "5",
					}),
				},
			}, nil),
			expected: `[{"decimal_col4": "-9999.99999"}, {"decimal_col4": "9999.99999"}]`,
		},
		{
			name:  "decimal5",
			query: "SELECT decimal_col5 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col5",
					Type:     &arrow.Decimal64Type{Precision: 18, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col5",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "18",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"decimal_col5": "-999999999999999999"}, {"decimal_col5": "999999999999999999"}]`,
		},
		{
			name:  "dec",
			query: "SELECT dec_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "dec_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "dec_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"dec_col": -9999999999}, {"dec_col": 9999999999}]`,
		},
		{
			name:  "fixed",
			query: "SELECT fixed_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "fixed_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "fixed_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"fixed_col": -9999999999}, {"fixed_col": 9999999999}]`,
		},
		{
			name:  "numeric",
			query: "SELECT numeric_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "numeric_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "numeric_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"numeric_col": -9999999999}, {"numeric_col": 9999999999}]`,
		},
		{
			name:  "date",
			query: "SELECT date_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "date_col",
					Type:     arrow.FixedWidthTypes.Date32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "date_col",
						"sql.database_type_name": "DATE",
					}),
				},
			}, nil),
			expected: `[{"date_col": "1000-01-01"}, {"date_col": "9999-12-31"}]`,
		},
		{
			name:  "time",
			query: "SELECT time_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "time_col",
					Type:     arrow.FixedWidthTypes.Time32s,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "time_col",
						"sql.database_type_name":           "TIME",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"time_col": -3020399}, {"time_col": 3020399}]`,
		},
		{
			name:  "time6",
			query: "SELECT time6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "time6_col",
					Type:     arrow.FixedWidthTypes.Time64us,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "time6_col",
						"sql.database_type_name":           "TIME",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"time6_col": -3020399000000}, {"time6_col": 3020398999999}]`,
		},
		{
			name:  "datetime",
			query: "SELECT datetime_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "datetime_col",
					Type:     &arrow.TimestampType{Unit: arrow.Second},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "datetime_col",
						"sql.database_type_name":           "DATETIME",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"datetime_col": "1000-01-01 00:00:00"}, {"datetime_col": "9999-12-31 23:59:59"}]`,
		},
		{
			name:  "datetime6",
			query: "SELECT datetime6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "datetime6_col",
					Type:     &arrow.TimestampType{Unit: arrow.Microsecond},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "datetime6_col",
						"sql.database_type_name":           "DATETIME",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"datetime6_col": "1000-01-01 00:00:00.000000"}, {"datetime6_col": "9999-12-31 23:59:59.999999"}]`,
		},
		{
			name:  "timestamp",
			query: "SELECT timestamp_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "timestamp_col",
					Type:     arrow.FixedWidthTypes.Timestamp_s,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "timestamp_col",
						"sql.database_type_name":           "TIMESTAMP",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"timestamp_col": "1970-01-01 00:00:01"}, {"timestamp_col": "2038-01-19 03:14:07"}]`,
		},
		{
			name:  "timestamp6",
			query: "SELECT timestamp6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "timestamp6_col",
					Type:     arrow.FixedWidthTypes.Timestamp_us,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "timestamp6_col",
						"sql.database_type_name":           "TIMESTAMP",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"timestamp6_col": "1970-01-01 00:00:01"}, {"timestamp6_col": "2038-01-19 03:14:06.999999"}]`,
		},
		{
			name:  "year",
			query: "SELECT year_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "year_col",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "year_col",
						"sql.database_type_name": "YEAR",
					}),
				},
			}, nil),
			expected: `[{"year_col": 1901}, {"year_col": 2155}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type SingleStoreTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *SingleStoreTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("SINGLESTORE_DSN")
	if s.dsn == "" {
		s.T().Skip("Set SINGLESTORE_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = singlestore.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *SingleStoreTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close())
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close())
	}
	if s.db != nil {
		s.NoError(s.db.Close())
	}
	s.mem.AssertSize(s.T(), 0)
}

func TestSingleStoreTypeTests(t *testing.T) {
	dsn := os.Getenv("SINGLESTORE_DSN")
	if dsn == "" {
		t.Skip("Set SINGLESTORE_DSN environment variable for type tests")
	}

	quirks := &SingleStoreQuirks{dsn: dsn}
	suite.Run(t, &SingleStoreTests{Quirks: quirks})
}

func TestSingleStoreIntegrationSuite(t *testing.T) {
	suite.Run(t, new(SingleStoreTestSuite))
}

// TestURIParsing tests the parseToSingleStoreDSN function with various URI formats
func TestURIParsing(t *testing.T) {
	factory := singlestore.NewSingleStoreDBFactory()

	tests := []struct {
		name           string
		singlestoreURI string
		username       string
		password       string
		expectedDSN    string
		shouldError    bool
		errorContains  string
	}{
		// TCP connection variations
		{
			name:           "basic tcp with port",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "tcp without port - should default to 3306",
			singlestoreURI: "mysql://user:pass@localhost/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "tcp without host - should be invalid",
			singlestoreURI: "mysql://user:pass@/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "tcp without database",
			singlestoreURI: "mysql://user:pass@localhost:3306",
			expectedDSN:    "user:pass@tcp(localhost:3306)/",
		},
		{
			name:           "tcp without database but with slash",
			singlestoreURI: "mysql://user:pass@localhost:3306/",
			expectedDSN:    "user:pass@tcp(localhost:3306)/",
		},
		{
			name:           "tcp with custom port",
			singlestoreURI: "mysql://user:pass@example.com:3307/myapp",
			expectedDSN:    "user:pass@tcp(example.com:3307)/myapp",
		},
		{
			name:           "tcp with ip address",
			singlestoreURI: "mysql://user:pass@127.0.0.1:3306/testdb",
			expectedDSN:    "user:pass@tcp(127.0.0.1:3306)/testdb",
		},
		{
			name:           "tcp with ipv6 host",
			singlestoreURI: "mysql://user:pass@[::1]:3306/testdb",
			expectedDSN:    "user:pass@tcp([::1]:3306)/testdb",
		},
		{
			name:           "tcp with ipv6 host, default port",
			singlestoreURI: "mysql://user:pass@[::1]/testdb",
			expectedDSN:    "user:pass@tcp([::1]:3306)/testdb",
		},

		// Credential handling variations
		{
			name:           "no credentials in uri",
			singlestoreURI: "mysql://localhost:3306/testdb",
			expectedDSN:    "tcp(localhost:3306)/testdb",
		},
		{
			name:           "only username in uri",
			singlestoreURI: "mysql://user@localhost:3306/testdb",
			expectedDSN:    "user@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override credentials with options",
			singlestoreURI: "mysql://olduser:oldpass@localhost:3306/testdb",
			username:       "newuser",
			password:       "newpass",
			expectedDSN:    "newuser:newpass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "add credentials via options",
			singlestoreURI: "mysql://localhost:3306/testdb",
			username:       "admin",
			password:       "secret",
			expectedDSN:    "admin:secret@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override only username",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			username:       "newuser",
			expectedDSN:    "newuser:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override only password",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			password:       "newpass",
			expectedDSN:    "user:newpass@tcp(localhost:3306)/testdb",
		},

		// Query parameter variations
		{
			name:           "single query parameter",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4",
		},
		{
			name:           "multiple query parameters",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4&timeout=30s&tls=false",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4&timeout=30s&tls=false",
		},
		{
			name:           "ssl parameters",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?tls=skip-verify&timeout=10s",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?tls=skip-verify&timeout=10s",
		},
		{
			name:           "url encoded database name",
			singlestoreURI: "mysql://user:pass@localhost:3306/test%20db?charset=utf8",
			expectedDSN:    "user:pass@tcp(localhost:3306)/test%20db?charset=utf8",
		},
		{
			name:           "query parameters with encoding",
			singlestoreURI: "mysql://user:pass@localhost/testdb?time_zone=%27%2B00%3A00%27",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?time_zone=%27%2B00%3A00%27",
		},

		// Unix socket variations
		{
			name:           "unix socket with parentheses",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/testdb",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/testdb",
		},
		{
			name:           "unix socket with percent encoding - should be invalid. Must use parenthesis",
			singlestoreURI: "mysql://user:pass@/tmp%2Fsinglestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "unix socket with complex path",
			singlestoreURI: "mysql://user:pass@(/var/run/mysqld/mysqld.sock)/myapp",
			expectedDSN:    "user:pass@unix(/var/run/mysqld/mysqld.sock)/myapp",
		},
		{
			name:           "unix socket without database",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/",
		},
		{
			name:           "unix socket with query params",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/testdb?charset=utf8mb4",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/testdb?charset=utf8mb4",
		},
		{
			name:           "unix socket with empty host (ambiguous) - should be invalid",
			singlestoreURI: "mysql://user:pass@/tmp/singlestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "invalid unix socket (missing parenthesis)",
			singlestoreURI: "mysql://user@(/tmp/singlestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing closing ')'",
		},
		{
			name:           "unix socket (paren) with encoded db name",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/my%20db?foo=bar",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/my%20db?foo=bar",
		},
		// Special characters and edge cases
		{
			name:           "credentials with special characters",
			singlestoreURI: "mysql://my%40user:p%40ss%24word@localhost:3306/testdb",
			expectedDSN:    "my@user:p@ss$word@tcp(localhost:3306)/testdb",
		},

		// Error cases
		{
			name:           "invalid singlestore uri format",
			singlestoreURI: "mysql://[invalid-uri",
			shouldError:    true,
			errorContains:  "invalid SingleStore URI format",
		},
		{
			name:           "invalid socket path encoding",
			singlestoreURI: "mysql://user:pass@%ZZ%invalid/testdb",
			shouldError:    true,
			errorContains:  "invalid SingleStore URI format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.singlestoreURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			result, err := factory.BuildSingleStoreDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.expectedDSN, result, "DSN should match expected value")
		})
	}
}
