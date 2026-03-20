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

import functools
import os
from pathlib import Path

from adbc_drivers_validation import model, quirks


class SingleStoreQuirks(model.DriverQuirks):
    name = "singlestore"
    driver = "adbc_driver_singlestore"
    driver_name = "ADBC Driver for SingleStore"
    vendor_name = "SingleStore"
    vendor_version = (
        os.getenv("SINGLESTORE_VERSION")
        + " (SingleStoreDB source distribution (compatible; MySQL Enterprise & MySQL Commercial))"
    )
    short_version = os.getenv("SINGLESTORE_VERSION")
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=False,  # TODO: PLAT-7827
        connection_set_current_catalog=True,
        connection_set_current_schema=True,
        get_objects=True,
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,  # TODO: PLAT-7798
        get_objects_constraints_unique=False,  # TODO: PLAT-7798
        statement_bind=True,
        statement_prepare=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=True,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        current_catalog="db",  # SingleStore treats databases as catalogs (also JDBC behavior)
        current_schema="",  # getSchemas() returns empty - no schema concept (also JDBC behavior)
        secondary_catalog="db2",
        secondary_schema="",
        secondary_catalog_schema="",
        supported_xdbc_fields=["xdbc_nullable"],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SINGLESTORE_DSN"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def bind_parameter(self, index: int) -> str:
        return "?"

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        # Check if the error indicates a table not found condition
        error_str = str(error).lower()
        return (
            "table" in error_str
            and (
                "does not exist" in error_str
                or "doesn't exist" in error_str
                or "not found" in error_str
            )
            and table_name.lower() in error_str
        )

    def quote_identifier(self, *identifiers: str) -> str:
        return ".".join(
            self.quote_one_identifier(ident) for ident in identifiers if ident
        )

    def quote_one_identifier(self, identifier: str) -> str:
        identifier = identifier.replace("`", "``")
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


@functools.cache
def get_quirks(version: str) -> model.DriverQuirks:
    return SingleStoreQuirks()
