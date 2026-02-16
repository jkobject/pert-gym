#!/usr/bin/env bash
set -euo pipefail
previous_tag=$(git tag --sort=-creatordate | sed -n 2p)
git shortlog "${previous_tag}.." | sed 's/^./    &/'
