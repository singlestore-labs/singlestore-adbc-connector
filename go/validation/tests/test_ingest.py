# Copyright (c) 2025 ADBC Drivers Contributors
# Copyright (c) 2026 SingleStore, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.ingest
import pyarrow
from adbc_drivers_validation import compare, model
from adbc_drivers_validation.utils import execute_query_without_prepare

from . import singlestore


def pytest_generate_tests(metafunc) -> None:
    return adbc_drivers_validation.tests.ingest.generate_tests(
        singlestore.QUIRKS, metafunc
    )


class TestIngest(adbc_drivers_validation.tests.ingest.TestIngest):
    def test_catalog(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        data_schema = pyarrow.schema(
            [("idx", pyarrow.int64()), ("value", pyarrow.large_string())]
        )
        data = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            },
            schema=data_schema,
        )
        table_name = "test_ingest_catalog"
        schema_name = driver.features.secondary_catalog_schema
        catalog_name = driver.features.secondary_catalog
        assert schema_name is not None
        assert catalog_name is not None
        with conn.cursor() as cursor:
            driver.try_drop_table(
                cursor,
                table_name=table_name,
                schema_name=schema_name,
                catalog_name=catalog_name,
            )
            cursor.adbc_ingest(
                table_name,
                data,
                db_schema_name=schema_name,
                catalog_name=catalog_name,
            )

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(catalog_name, schema_name, table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            result = execute_query_without_prepare(cursor, select)

        compare.compare_tables(data, result)

    def test_replace_catalog(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        data_schema = pyarrow.schema(
            [("idx", pyarrow.int64()), ("value", pyarrow.large_string())]
        )

        # Create a table in the default catalog/schema
        default_data = pyarrow.Table.from_pydict(
            {
                "idx": [10, 20, 30],
                "value": ["original", "default", "catalog"],
            },
            schema=data_schema,
        )
        # Create different data for the secondary catalog
        data = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            },
            schema=data_schema,
        )
        data2 = data.slice(0, 1)

        table_name = "test_ingest_replace_catalog"
        schema_name = driver.features.secondary_catalog_schema
        catalog_name = driver.features.secondary_catalog
        assert schema_name is not None
        assert catalog_name is not None

        with conn.cursor() as cursor:
            # Create table in default catalog/schema
            driver.try_drop_table(cursor, table_name=table_name)
            cursor.adbc_ingest(table_name, default_data, mode="create")

            # Create and replace table in secondary catalog
            driver.try_drop_table(
                cursor,
                table_name=table_name,
                schema_name=schema_name,
                catalog_name=catalog_name,
            )
            cursor.adbc_ingest(
                table_name,
                data,
                mode="replace",
                db_schema_name=schema_name,
                catalog_name=catalog_name,
            )

            if driver.name == "bigquery":
                # BigQuery rate-limits metadata operations
                time.sleep(5)

            # Replace again with smaller dataset in secondary catalog
            modified = cursor.adbc_ingest(
                table_name,
                data2,
                mode="replace",
                db_schema_name=schema_name,
                catalog_name=catalog_name,
            )
            if driver.features.statement_rows_affected:
                assert modified == len(data2)
            else:
                assert modified == -1

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")

        # Verify secondary catalog table has the replaced data
        select_secondary = f"SELECT {idx}, {value} FROM {driver.quote_identifier(catalog_name, schema_name, table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            result_secondary = execute_query_without_prepare(cursor, select_secondary)
        expected_secondary = data.slice(0, 1)
        compare.compare_tables(expected_secondary, result_secondary)

        # Verify default catalog/schema table is unchanged
        select_default = f"SELECT {idx}, {value} FROM {driver.quote_identifier(table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            result_default = execute_query_without_prepare(cursor, select_default)
        compare.compare_tables(default_data, result_default)
