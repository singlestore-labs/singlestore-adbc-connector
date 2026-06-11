<!--
  Copyright (c) 2025 ADBC Drivers Contributors
  Copyright (c) 2026 SingleStore, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Driver for SingleStore

An [ADBC driver](https://arrow.apache.org/adbc/) for SingleStore.

## Installation

Pre-packaged builds of the drivers in this repo have been made available for
various platforms from the [Columnar](https://columnar.tech) CDN. These can be
installed by any tool that supports [ADBC](https://arrow.apache.org/adbc/)
Driver Manifests, such as [dbc](https://columnar.tech/dbc):

```sh
dbc install singlestore
```

See [Building](#building) if you would rather build the drivers yourself.

<!-- TODO: Create examples of SingleStore ADBC driver usage in various languages, and link to them here.
## Usage

See examples for:

- [Go](https://github.com/columnar-tech/adbc-quickstarts/tree/main/go/mysql)
- [Python](https://github.com/columnar-tech/adbc-quickstarts/tree/main/python/mysql)
- [R](https://github.com/columnar-tech/adbc-quickstarts/tree/main/r/mysql)
- [Rust](https://github.com/columnar-tech/adbc-quickstarts/tree/main/rust/mysql)
-->

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Release process

To release a new version:

1. Push a version tag using semantic versioning with a `go/v` prefix (`go/v<major>.<minor>.<patch>`, for example `go/v1.2.3`):

   ```bash
   git tag go/v1.0.1
   git push origin go/v1.0.1
   ```

   This triggers the [Go Release workflow](.github/workflows/go_release.yaml), which:

   - Builds the driver for Linux, macOS, and Windows
   - Creates a [GitHub Release](https://github.com/singlestore-labs/singlestore-adbc-connector/releases) with platform packages (`.tar.gz` archives), an ADBC driver manifest (`manifest.yaml`), and generated documentation
   - Uploads the release assets to GitHub

2. After the workflow completes, open the draft release on GitHub, modify it if needed, and make it publicly available.

3. To make the new version installable via [dbc](https://columnar.tech/dbc/), email the maintainers at [david@columnar.tech](mailto:david@columnar.tech) and [ian@columnar.tech](mailto:ian@columnar.tech). You can use the following email as a template — replace `<version>` with the release version (for example, `1.0.1` for tag `go/v1.0.1`):

   ```
   Hi,

   We have released a new version of the SingleStore ADBC driver.
   You can access the release at the following link:
   https://github.com/singlestore-labs/singlestore-adbc-connector/releases/tag/go%2Fv<version>

   Please update the driver in dbc so users can install it with:

     dbc install singlestore

   Let me know if you need any additional information or if any further steps are required.

   Best regards,
   <Your name>
   ```
