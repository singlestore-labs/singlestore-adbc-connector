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
    quirks = [singlestore.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.ingest.generate_tests(quirks, metafunc)


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
