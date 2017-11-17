#!/bin/bash

set -e
set -u

echo "======== Load wdlabel ==========="

time go run /wof/code/wdlabelparse.go

echo "======== end of wdlabel ==========="
