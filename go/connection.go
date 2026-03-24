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
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
	mysql "github.com/memsql/go-singlestore-driver"
)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *singlestoreConnectionImpl) GetCurrentCatalog() (string, error) {
	var database string
	err := c.Db.QueryRowContext(context.Background(), "SELECT DATABASE()").Scan(&database)
	if err != nil {
		return "", c.ErrorHelper.WrapIO(err, "failed to get current database")
	}
	if database == "" {
		return "", c.ErrorHelper.InvalidState("no current database set")
	}
	return database, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *singlestoreConnectionImpl) GetCurrentDbSchema() (string, error) {
	return "", nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *singlestoreConnectionImpl) SetCurrentCatalog(catalog string) error {
	_, err := c.Db.ExecContext(context.Background(), "USE "+c.QuoteIdentifier(catalog))
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *singlestoreConnectionImpl) SetCurrentDbSchema(schema string) error {
	if schema != "" {
		return c.ErrorHelper.InvalidArgument("cannot set schema in SingleStore: schemas are not supported")
	}
	return nil
}

func (c *singlestoreConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version, comment string
		if err := c.Conn.QueryRowContext(ctx, "SELECT @@memsql_version, @@version_comment").Scan(&version, &comment); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get version")
		}
		c.version = fmt.Sprintf("%s (%s)", version, comment)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// GetTableSchema returns the Arrow schema for a SingleStore table
func (c *singlestoreConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	// Struct to capture SingleStore column information
	type tableColumn struct {
		OrdinalPosition        int32
		ColumnName             string
		DataType               string
		IsNullable             string
		CharacterMaximumLength sql.NullInt64
		NumericPrecision       sql.NullInt64
		NumericScale           sql.NullInt64
	}

	query := `SELECT
		ORDINAL_POSITION,
		COLUMN_NAME,
		DATA_TYPE,
		IS_NULLABLE,
		CHARACTER_MAXIMUM_LENGTH,
		NUMERIC_PRECISION,
		NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	ORDER BY ORDINAL_POSITION`

	var args []any
	if catalog != nil && *catalog != "" {
		// Use specified catalog (database)
		args = []any{*catalog, tableName}
	} else {
		// Use current database
		currentDB, err := c.GetCurrentCatalog()
		if err != nil {
			return nil, err
		}
		args = []any{currentDB, tableName}
	}

	// Execute query to get column information
	rows, err := c.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query table schema")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var columns []tableColumn
	for rows.Next() {
		var col tableColumn
		err := rows.Scan(
			&col.OrdinalPosition,
			&col.ColumnName,
			&col.DataType,
			&col.IsNullable,
			&col.CharacterMaximumLength,
			&col.NumericPrecision,
			&col.NumericScale,
		)
		if err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan column information")
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "rows error")
	}

	if len(columns) == 0 {
		return nil, c.ErrorHelper.NotFound("table not found: %s", tableName)
	}

	// Build Arrow schema from column information using type converter
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		// Create ColumnType struct for the type converter
		var length, precision, scale *int64
		if col.CharacterMaximumLength.Valid {
			length = &col.CharacterMaximumLength.Int64
		}
		if col.NumericPrecision.Valid {
			precision = &col.NumericPrecision.Int64
		}
		if col.NumericScale.Valid {
			scale = &col.NumericScale.Int64
		}

		colType := sqlwrapper.ColumnType{
			Name:             col.ColumnName,
			DatabaseTypeName: col.DataType,
			Nullable:         col.IsNullable == "YES",
			Length:           length,
			Precision:        precision,
			Scale:            scale,
		}

		arrowType, nullable, metadata, err := c.TypeConverter.ConvertRawColumnType(colType)
		if err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to convert column type for %s", col.ColumnName)
		}

		fields[i] = arrow.Field{
			Name:     col.ColumnName,
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// ListTableTypes implements driverbase.TableTypeLister interface
func (c *singlestoreConnectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// SingleStore supports these standard table types
	return []string{
		"BASE TABLE",             // Regular tables
		"VIEW",                   // Views
		"SYSTEM VIEW",            // System/information schema views
		"TEMPORARY TABLE",        // Session-scoped temporary tables
		"GLOBAL TEMPORARY TABLE", // Global temporary tables
	}, nil
}

func generateReaderName(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "stream_" + hex.EncodeToString(b), nil
}

// streamArrowAsCSV takes an Arrow RecordReader and streams it out as CSV data.
// It returns an io.Reader that the SingleStore driver can consume immediately.
func (c *singlestoreConnectionImpl) streamArrowAsCSV(stream array.RecordReader, logger *slog.Logger) io.Reader {
	pr, pw := io.Pipe()

	// Spin up a goroutine to write the Arrow records to the pipe
	go func() {
		defer func() {
			if err := pw.Close(); err != nil {
				logger.Warn(fmt.Sprintf("Error closing pipe: %v", err))
			}
		}()

		w := arrowcsv.NewWriter(pw, stream.Schema(),
			arrowcsv.WithBoolWriter(func(b bool) string {
				if b {
					return "1"
				}
				return "0"
			}),
			arrowcsv.WithLazyQuotes(false),
			arrowcsv.WithHeader(false),
			arrowcsv.WithCustomTypeConverter(ConvertArrowToCSV),
		)

		// Ensure the CSV writer flushes any remaining buffer on exit
		defer func() {
			if err := w.Flush(); err != nil {
				logger.Warn(fmt.Sprintf("Error flushing CSV writer: %v", err))
			}
		}()

		for stream.Next() {
			rec := stream.RecordBatch()
			if err := w.Write(rec); err != nil {
				if err := pw.CloseWithError(fmt.Errorf("failed to write arrow record to csv: %w", err)); err != nil {
					logger.Warn(fmt.Sprintf("Error closing pipe: %v", err))
				}
				return
			}
		}

		if err := stream.Err(); err != nil {
			if err := pw.CloseWithError(fmt.Errorf("arrow record reader error: %w", err)); err != nil {
				logger.Warn(fmt.Sprintf("Error closing pipe: %v", err))
			}
			return
		}
	}()

	return pr
}

func (c *singlestoreConnectionImpl) generateTmpVarName(column string) string {
	return fmt.Sprintf("@%s", c.QuoteIdentifier(column+"_tmp"))
}

func (c *singlestoreConnectionImpl) generateSetClause(schema *arrow.Schema) (string, string) {
	columns := make([]string, 0)
	setClauses := make([]string, 0)

	for _, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
			varName := c.generateTmpVarName(field.Name)
			columns = append(columns, varName)

			setExpr := fmt.Sprintf("%s = FROM_BASE64(%s)", c.QuoteIdentifier(field.Name), varName)
			setClauses = append(setClauses, setExpr)
		default:
			columns = append(columns, c.QuoteIdentifier(field.Name))
		}
	}

	setClause := strings.Join(setClauses, ", ")
	if setClause != "" {
		setClause = "SET " + setClause
	}

	return strings.Join(columns, ", "), setClause
}

// ExecuteBulkIngest performs SingleStore bulk ingest using INSERT statements
func (c *singlestoreConnectionImpl) ExecuteBulkIngest(ctx context.Context, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	if stream == nil {
		return -1, c.ErrorHelper.InvalidArgument("stream cannot be nil")
	}

	// Get schema from the stream and create a table if needed
	schema := stream.Schema()
	if err := c.createTableIfNeeded(ctx, conn, options.CatalogName, options.TableName, schema, options); err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to create table")
	}

	readerName, err := generateReaderName(10)
	if err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to generate reader name")
	}

	mysql.RegisterReaderHandler(readerName, func() io.Reader {
		return c.streamArrowAsCSV(stream, conn.Logger)
	})
	defer mysql.DeregisterReaderHandler(readerName)

	columns, setClauses := c.generateSetClause(schema)
	loadDataQuery := fmt.Sprintf("LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s "+
		"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "+
		"ESCAPED BY '' "+
		"LINES TERMINATED BY '\n' "+
		"NULL DEFINED BY 'NULL' "+
		"(%s) "+
		"%s",
		readerName,
		c.QuoteTableID(options.CatalogName, options.TableName),
		columns,
		setClauses,
	)

	result, err := conn.ExecContext(ctx, loadDataQuery)
	if err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to load data")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to get rows affected")
	}

	return rowsAffected, nil
}

// GetPlaceholder returns the SQL placeholder for a field at the given parameter index (0-based)
func (c *singlestoreConnectionImpl) GetPlaceholder(field *arrow.Field, index int) string {
	return "?"
}

// QuoteTableID quotes a table identifier for SQL
func (c *singlestoreConnectionImpl) QuoteTableID(database string, table string) string {
	if database == "" {
		return c.QuoteIdentifier(table)
	}

	return c.QuoteIdentifier(database) + "." + c.QuoteIdentifier(table)
}

// QuoteIdentifier quotes a database/table/column identifier for SQL
func (c *singlestoreConnectionImpl) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// createTableIfNeeded creates the table based on the ingest mode
func (c *singlestoreConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, catalogName string, tableName string, schema *arrow.Schema, options *driverbase.BulkIngestOptions) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, catalogName, tableName, schema, false, options.Temporary)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, catalogName, tableName, schema, true, options.Temporary)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, catalogName, tableName); err != nil {
			return err
		}
		return c.createTable(ctx, conn, catalogName, tableName, schema, false, options.Temporary)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a SingleStore table from Arrow schema
func (c *singlestoreConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, catalogName string, tableName string, schema *arrow.Schema, ifNotExists bool, temporary bool) error {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("CREATE ")
	if temporary {
		queryBuilder.WriteString("TEMPORARY ")
	}
	queryBuilder.WriteString("TABLE ")

	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString(c.QuoteTableID(catalogName, tableName))
	queryBuilder.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(c.QuoteIdentifier(field.Name))
		queryBuilder.WriteString(" ")

		// Convert Arrow type to SingleStore type
		singlestoreType := c.arrowToSingleStoreType(field.Type, field.Nullable)
		queryBuilder.WriteString(singlestoreType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String())
	return err
}

// dropTable drops a SingleStore table
func (c *singlestoreConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, catalogName string, tableName string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", c.QuoteTableID(catalogName, tableName))
	_, err := conn.ExecContext(ctx, dropSQL)
	return err
}

// arrowToSingleStoreType converts Arrow data type to SingleStore column type
func (c *singlestoreConnectionImpl) arrowToSingleStoreType(arrowType arrow.DataType, nullable bool) string {
	var singlestoreType string

	switch arrowType := arrowType.(type) {
	case *arrow.BooleanType:
		singlestoreType = "BOOLEAN"
	case *arrow.Int8Type:
		singlestoreType = "TINYINT"
	case *arrow.Int16Type:
		singlestoreType = "SMALLINT"
	case *arrow.Int32Type:
		singlestoreType = "INT"
	case *arrow.Int64Type:
		singlestoreType = "BIGINT"
	case *arrow.Float32Type:
		singlestoreType = "FLOAT"
	case *arrow.Float64Type:
		singlestoreType = "DOUBLE"
	case *arrow.StringType, *arrow.LargeStringType:
		singlestoreType = "LONGTEXT"
	case *arrow.BinaryType, *arrow.FixedSizeBinaryType, *arrow.BinaryViewType, *arrow.LargeBinaryType:
		singlestoreType = "LONGBLOB"
	case *arrow.Date32Type:
		singlestoreType = "DATE"
	case *arrow.TimestampType:

		// Determine precision based on Arrow timestamp unit
		var precision string
		switch arrowType.Unit {
		case arrow.Second:
			precision = ""
		case arrow.Millisecond, arrow.Microsecond, arrow.Nanosecond:
			precision = "(6)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Arrow timestamp unit: %v", arrowType.Unit))
		}

		// Use DATETIME for timezone-naive timestamps, TIMESTAMP for timezone-aware
		if arrowType.TimeZone != "" {
			// Timezone-aware (timestamptz) -> TIMESTAMP
			singlestoreType = "TIMESTAMP" + precision
		} else {
			// Timezone-naive (timestamp) -> DATETIME
			singlestoreType = "DATETIME" + precision
		}
	case *arrow.Time32Type:
		// Determine precision based on Arrow time unit
		switch arrowType.Unit {
		case arrow.Second:
			singlestoreType = "TIME"
		case arrow.Millisecond:
			singlestoreType = "TIME(6)"
		default:
			// should never happen, but panic here for defensive programming
			panic(fmt.Sprintf("unexpected Time32 unit: %v", arrowType.Unit))
		}

	case *arrow.Time64Type:
		if arrowType.Unit != arrow.Microsecond && arrowType.Unit != arrow.Nanosecond {
			panic(fmt.Sprintf("unexpected Time64 unit: %v", arrowType.Unit))
		}
		singlestoreType = "TIME(6)"
	case arrow.DecimalType:
		singlestoreType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to TEXT for unknown types
		singlestoreType = "TEXT"
	}

	if nullable {
		singlestoreType += " NULL"
	} else {
		singlestoreType += " NOT NULL"
	}

	return singlestoreType
}

func (c *singlestoreConnectionImpl) SetOption(key string, val string) error {
	if strings.ToLower(key) == adbc.OptionKeyAutoCommit {
		switch strings.ToLower(val) {
		case adbc.OptionValueEnabled:
			if c.Conn != nil {
				_, err := c.Conn.ExecContext(context.Background(), "SET AUTOCOMMIT = 1")
				if err == nil {
					c.Autocommit = true
				}
				return err
			}

			c.Autocommit = true
			return nil
		case adbc.OptionValueDisabled:
			if c.Conn != nil {
				_, err := c.Conn.ExecContext(context.Background(), "SET AUTOCOMMIT = 0")
				if err == nil {
					c.Autocommit = false
				}
				return err
			}

			c.Autocommit = false
			return nil
		default:
			return c.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "invalid value for autocommit option: expected 'true' or 'false', got '%s'", val)
		}
	}

	return c.ConnectionImplBase.SetOption(key, val)
}

func (c *singlestoreConnectionImpl) Commit(ctx context.Context) error {
	if c.Autocommit {
		return c.Base().ErrorHelper.Errorf(
			adbc.StatusInvalidState,
			"Commit not supported in auto-commit mode",
		)
	}

	_, err := c.Conn.ExecContext(ctx, "COMMIT")
	return err
}

func (c *singlestoreConnectionImpl) Rollback(ctx context.Context) error {
	if c.Autocommit {
		return c.Base().ErrorHelper.Errorf(
			adbc.StatusInvalidState,
			"Rollback not supported in auto-commit mode",
		)
	}

	_, err := c.Conn.ExecContext(ctx, "ROLLBACK")
	return err
}
