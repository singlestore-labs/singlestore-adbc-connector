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
	"github.com/apache/arrow-go/v18/arrow/extensions"
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
			bool_col TINYINT(1),
			tinyint_col TINYINT,
			int_col INT,
			bigint_col BIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR(100),
			json_col JSON,
			enum_col ENUM('active', 'inactive'),
			point_col GEOGRAPHYPOINT,
			polygon_col GEOGRAPHY,
			geometry_col GEOGRAPHY,
			bit_col BIT(8)
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data including spatial data
	s.NoError(s.stmt.SetSqlQuery(`
		INSERT INTO test_types VALUES (
			1, 42, 12345, 9876543210, 3.25, 6.75, 'hello world',
			'{"key": "value", "number": 42}', 'active',
			'POINT(1 2)',
			'POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))',
			'LINESTRING(0 0, 1 1, 2 2)',
			b'10101010'
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "istrue",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"istrue": 1}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"value": 42}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 12345}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "FLOAT",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "DOUBLE",
						"sql.precision":          "9223372036854775807",
						"sql.scale":              "9223372036854775807",
					}),
				},
			}, nil),
			expected: `[{"value": 6.75}]`,
		},
		{
			name:  "string",
			query: "SELECT varchar_col AS greeting FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "greeting",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"greeting": "hello world"}]`,
		},
		{
			name:  "json",
			query: "SELECT json_col AS data FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "data",
					Type:     func() arrow.DataType { t, _ := extensions.NewJSONType(arrow.BinaryTypes.String); return t }(),
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "data",
						"sql.database_type_name": "JSON",
					}),
				},
			}, nil),
			expected: `[{"data": "{\"key\": \"value\", \"number\": 42}"}]`,
		},
		{
			name:  "enum",
			query: "SELECT enum_col AS status FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "status",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":         "status",
						"sql.database_type_name":  "ENUM",
						"singlestore.is_enum_set": "true",
					}),
				},
			}, nil),
			expected: `[{"status": "active"}]`,
		},
		{
			name:  "point",
			query: "SELECT point_col AS location FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "location",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "location",
						"sql.database_type_name": "GEOMETRY",
						"singlestore.is_spatial": "true",
					}),
				},
			}, nil),
			expected: `[{"location": "AAAAAAEBAAAAAAAAAAAA8D8AAAAAAAAAQA=="}]`,
		},
		{
			name:  "polygon",
			query: "SELECT polygon_col AS area FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "area",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "area",
						"sql.database_type_name": "GEOMETRY",
						"singlestore.is_spatial": "true",
					}),
				},
			}, nil),
			expected: `[{"area": "AAAAAAEDAAAAAQAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIQAAAAAAAAAhAAAAAAAAACEAAAAAAAAAIQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="}]`,
		},
		{
			name:  "geometry",
			query: "SELECT geometry_col AS shape FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "shape",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "shape",
						"sql.database_type_name": "GEOMETRY",
						"singlestore.is_spatial": "true",
					}),
				},
			}, nil),
			expected: `[{"shape": "AAAAAAECAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADwPwAAAAAAAPA/AAAAAAAAAEAAAAAAAAAAQA=="}]`,
		},
		{
			name:  "bit8",
			query: "SELECT bit_col AS bitvalue FROM test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bitvalue",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bitvalue",
						"sql.database_type_name": "BIT",
					}),
				},
			}, nil),
			expected: `[{"bitvalue": "qg=="}]`,
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
