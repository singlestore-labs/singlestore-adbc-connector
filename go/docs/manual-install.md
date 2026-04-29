# Manually Installing the SingleStore ADBC Driver

This document explains how to install the SingleStore ADBC driver from a locally built shared library so that the ADBC driver manager can load it by name (e.g. `driver="singlestore"`).

For most users the recommended path is `dbc install singlestore`. Use these instructions when you are developing the driver, testing a local build, or working in an environment where `dbc` is not available.

## Overview

The ADBC driver manager locates drivers via TOML **manifest** files. A manifest describes the driver and points to the compiled shared library for each supported architecture.

A proper install consists of two files:

1. The compiled shared library (the `build/` output from `pixi run make`).
2. A manifest (`singlestore.toml`) placed in a directory the driver manager searches.

## Build artifacts

After running the build you will have a shared library under `go/build/` with a platform-specific extension:

| Platform | File |
|---|---|
| macOS  | `libadbc_driver_singlestore.dylib` |
| Linux  | `libadbc_driver_singlestore.so` |

## Where the driver manager looks for manifests

The search paths are platform-specific. The driver manager tries them in order and uses the first manifest matching the requested driver name.

### macOS

- User-level: `~/Library/Application Support/ADBC/Drivers/`
- System-level: `/Library/Application Support/ADBC/Drivers/`
- Any directory listed in the `ADBC_DRIVER_PATH` environment variable (colon-separated).
- `<venv>/etc/adbc/drivers/` when running inside a Python virtualenv.

### Linux

- User-level: `${XDG_CONFIG_HOME:-$HOME/.config}/adbc/`
- System-level: `/etc/adbc/`
- Any directory listed in the `ADBC_DRIVER_PATH` environment variable (colon-separated).
- `<venv>/etc/adbc/drivers/` when running inside a Python virtualenv.

## Architecture keys in the manifest

The `[Driver.shared]` table maps architecture keys to absolute library paths. The driver manager reports its current architecture as `<os>_<arch>` and looks up that exact key.

| Host | Key |
|---|---|
| macOS Intel | `macos_amd64` |
| macOS Apple Silicon | `macos_arm64` |
| Linux x86_64 | `linux_amd64` |
| Linux ARM64 | `linux_arm64` |
| Windows x86_64 | `windows_amd64` |

You can list multiple architectures in one manifest; the manager picks the right one at load time.

---

## macOS: step-by-step

Assumes you have already built the driver and `go/build/libadbc_driver_singlestore.dylib` exists.

```bash
# 1. Pick a place for the shared library. Any absolute path works; this is just
#    a convention that keeps things together.
mkdir -p ~/.local/lib/adbc

# 2. Copy the built dylib.
cp go/build/libadbc_driver_singlestore.dylib ~/.local/lib/adbc/

# 3. Create the manifest directory (macOS-specific location).
mkdir -p "$HOME/Library/Application Support/ADBC/Drivers"

# 4. Write the manifest. Replace the key if you are on Apple Silicon
#    (use `macos_arm64` instead of `macos_amd64`).
cat > "$HOME/Library/Application Support/ADBC/Drivers/singlestore.toml" <<EOF
name = "ADBC Driver for SingleStore"
description = "An ADBC Driver for SingleStore"
publisher = "SingleStore"
license = "Apache-2.0"

[ADBC]
version = "v1.1.0"

[Driver.shared]
macos_amd64 = "$HOME/.local/lib/adbc/libadbc_driver_singlestore.dylib"
EOF
```

To check your host architecture: `uname -m` — `x86_64` maps to `macos_amd64`, `arm64` maps to `macos_arm64`.

## Linux: step-by-step

Assumes you have already built the driver and `go/build/libadbc_driver_singlestore.so` exists.

```bash
# 1. Pick a place for the shared library.
mkdir -p ~/.local/lib/adbc

# 2. Copy the built .so.
cp go/build/libadbc_driver_singlestore.so ~/.local/lib/adbc/

# 3. Create the manifest directory (Linux user-level location).
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/adbc"

# 4. Write the manifest. Replace the key with `linux_arm64` if you are on ARM64.
cat > "${XDG_CONFIG_HOME:-$HOME/.config}/adbc/singlestore.toml" <<EOF
name = "ADBC Driver for SingleStore"
description = "An ADBC Driver for SingleStore"
publisher = "SingleStore"
license = "Apache-2.0"

[ADBC]
version = "v1.1.0"

[Driver.shared]
linux_amd64 = "$HOME/.local/lib/adbc/libadbc_driver_singlestore.so"
EOF
```

To check your host architecture: `uname -m` — `x86_64` maps to `linux_amd64`, `aarch64` maps to `linux_arm64`.

## Verifying the install

From Python:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver="singlestore",
    db_kwargs={"uri": "root@tcp(localhost:3306)/demo"},
)
```

If the driver manager cannot load the driver, its error message lists every search path it tried and which architecture keys it found in the manifest — useful for diagnosing path or architecture-key mismatches.

## Troubleshooting

- **`Driver path not found in manifest ... for current architecture '<key>'`** — the key in your `[Driver.shared]` table does not match the host. Copy the exact key from the error message into the manifest.
- **Manifest not discovered at all** — confirm the file is in one of the search paths listed above for your OS, or set `ADBC_DRIVER_PATH` to its parent directory.
- **`dlopen()` / `ld.so` errors** — the path in the manifest must be absolute and the file must be readable. Symlinks are fine.

## Uninstalling

Delete the manifest and the shared library:

```bash
# macOS
rm "$HOME/Library/Application Support/ADBC/Drivers/singlestore.toml"

# Linux
rm "${XDG_CONFIG_HOME:-$HOME/.config}/adbc/singlestore.toml"

# Both
rm ~/.local/lib/adbc/libadbc_driver_singlestore.*
```
