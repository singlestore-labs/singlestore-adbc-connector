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

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.connection
from adbc_drivers_validation import compare, model

from . import singlestore


def pytest_generate_tests(metafunc) -> None:
    quirks = [singlestore.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    def test_get_objects_constraints_unique(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_objects_constraints: None,
    ) -> None:
        tables = self.get_constraints(driver, conn, "constraint_unique%")

        assert len(tables["constraint_unique"]) == 2
        constraints = list(
            sorted(
                tables["constraint_unique"],
                key=lambda x: len(x["constraint_column_names"]),
            )
        )
        compare.match_fields(
            constraints[0],
            {
                "constraint_type": "UNIQUE",
                "constraint_column_names": ["a"],
                "constraint_column_usage": None,
            },
        )

        # Even if declared as UNIQUE(c, b), some databases return [b, c]
        compare.match_fields(
            constraints[1],
            {
                "constraint_type": "UNIQUE",
                "constraint_column_usage": None,
            },
        )
        if driver.features.quirk_get_objects_constraints_unique_normalized:
            assert constraints[1]["constraint_column_names"] == ["a", "b"]
        else:
            assert constraints[1]["constraint_column_names"] == ["b", "a"]
