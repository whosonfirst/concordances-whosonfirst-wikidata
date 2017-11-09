#!/bin/bash
set -e
set -u

cd /wof/whosonfirst-data

git pull
git lfs fetch
git lfs checkout
