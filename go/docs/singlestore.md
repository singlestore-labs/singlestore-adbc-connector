---
# Copyright (c) 2025 ADBC Drivers Contributors
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
{}
---

{{ cross_reference|safe }}
# SingleStore Driver {{ version }}

{{ heading|safe }}

This driver provides access to [SingleStore][singlestore]{target="_blank"} relational database management system.

## Installation

The SingleStore driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install singlestore
```

## Connecting

To connect, edit the `uri` option below to match your environment and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
  driver="singlestore",
  db_kwargs = {
    "uri": "root@tcp(localhost:3306)/demo"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.

### Connection String Format

Connection strings are passed with the `uri` option which uses the following format:

```text
mysql://[user[:[password]]@]host[:port][/schema][?attribute1=value1&attribute2=value2...]
```

Examples:

- `mysql://localhost/mydb`
- `mysql://user:pass@localhost:3306/mydb`
- `mysql://user:pass@host/db?charset=utf8mb4&timeout=30s`
- `mysql://user@(/path/to/socket.sock)/db` (Unix domain socket)
- `mysql://user@localhost/mydb` (no password)

This follows MySQL's official [URI-like connection string format](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). Also see [MySQL Connection Parameters](https://dev.mysql.com/doc/refman/8.4/en/connecting-using-uri-or-key-value-pairs.html#connection-parameters-base) for the complete specification.

Components:
- `scheme`: `mysql://` (required)
- `user`: Optional (for authentication)
- `password`: Optional (for authentication, requires user)
- `host`: Required (must be explicitly specified)
- `port`: Optional (defaults to 3306)
- `schema`: Optional (can be empty, SingleStore database name)
- Query params: SingleStore connection attributes

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

When connecting via Unix domain sockets, use the parentheses syntax to wrap the socket path: `(/path/to/socket.sock)`.

The driver also supports the SingleStore DSN format (see [Go SingleStore Driver documentation](https://github.com/singlestore-labs/go-singlestore-driver?tab=readme-ov-file#dsn-data-source-name)), but standard URIs are recommended.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[singlestore]: https://www.singlestore.com/
