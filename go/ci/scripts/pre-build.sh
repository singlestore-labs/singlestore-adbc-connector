#!/usr/bin/env bash
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

# Hook run by the Go test/release workflows before the shared library is built.
#
# The version reported by adbc_get_info() is derived from the git tag, and the
# ADBC validation suite only accepts versions of the form
# vMAJOR[.MINOR[.PATCH]][-PRERELEASE]. A tag like go/v0.1.0b2 builds and
# packages fine but fails validation halfway through the release, so reject it
# before anything is built.

set -euo pipefail

# Semantic versioning, with the go/v prefix the release workflow triggers on.
TAG_PATTERN='^go/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'

tags=()

if [[ "${GITHUB_REF_TYPE:-}" == "tag" && -n "${GITHUB_REF_NAME:-}" ]]; then
  tags+=("${GITHUB_REF_NAME}")
fi

# Tags on HEAD matter too: the build picks the highest version among all tags,
# so a bad tag left on the commit being released can win over a good one.
while IFS= read -r tag; do
  if [[ -n "${tag}" && "${tag}" != "${GITHUB_REF_NAME:-}" ]]; then
    tags+=("${tag}")
  fi
done < <(git tag --points-at HEAD --list 'go/v*')

invalid=()
for tag in ${tags[@]+"${tags[@]}"}; do
  if [[ ! "${tag}" =~ ${TAG_PATTERN} ]]; then
    invalid+=("${tag}")
  fi
done

if [[ ${#invalid[@]} -gt 0 ]]; then
  {
    echo "Invalid release tag(s): ${invalid[*]}"
    echo
    echo "Release tags must be go/vMAJOR.MINOR.PATCH with an optional"
    echo "hyphen-separated pre-release suffix, for example:"
    echo
    echo "    go/v1.2.3"
    echo "    go/v1.2.3-beta.1"
    echo "    go/v1.2.3-rc.1"
    echo
    echo "Python-style suffixes such as go/v1.2.3b1 are rejected by the ADBC"
    echo "validation suite (test_get_info), which checks the driver version"
    echo "reported by the built library."
    echo
    echo "Delete the tag and push a corrected one:"
    echo
    for tag in "${invalid[@]}"; do
      echo "    git tag -d ${tag} && git push origin :refs/tags/${tag}"
    done
  } >&2
  exit 1
fi

if [[ ${#tags[@]} -gt 0 ]]; then
  echo "Building tagged version: ${tags[*]}"
fi
