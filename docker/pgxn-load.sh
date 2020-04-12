#!/bin/bash
set -o errexit

echo "install plpythonu extension "
psql  -c "CREATE EXTENSION if not exists plpythonu CASCADE"

#echo "install Madlib"
#pgxn load madlib