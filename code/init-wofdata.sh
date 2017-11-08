#!/bin/bash
set -e
set -u


cd /wof
git clone --depth 1 https://github.com/whosonfirst-data/whosonfirst-data.git
cd whosonfirst-data
#git lfs ls-files
git lfs fetch
git lfs checkout
