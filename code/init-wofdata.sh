#!/bin/bash
set -e
set -u


cd /wof
git clone --depth 1 -b master --single-branch https://github.com/whosonfirst-data/whosonfirst-data.git

# git clone --depth 1  -b master --single-branch   https://github.com/ImreSamu/whosonfirst-data.git
# cd whosonfirst-data
# git remote add upstream https://github.com/whosonfirst-data/whosonfirst-data.git
# git fetch upstream
# git pull  upstream master

cd whosonfirst-data
#git lfs ls-files
git lfs fetch
git lfs checkout

/wof/tools/go-whosonfirst-meta/bin/wof-build-metafiles
# https://github.com/whosonfirst-data/whosonfirst-data/raw/9e88f0ebdbf239bc78f278074e21000b4abb0bd8/utils/linux/wof-build-metafiles

#utils/linux/wof-build-metafiles

utils/linux/wof-build-concordances
    