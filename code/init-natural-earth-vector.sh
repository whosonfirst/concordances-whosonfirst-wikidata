#!/bin/bash
set -e
set -u


cd /wof
git clone --depth 1 --branch v5-prequel https://github.com/nvkelso/natural-earth-vector.git
