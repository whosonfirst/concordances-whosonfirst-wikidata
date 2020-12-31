#!/bin/bash
set -o errexit


nproc=$(nproc)
db_connection=$(( $nproc + 30))

python3 ./pgtune.py  -c ${db_connection}  > $PGDATA/postgresql.conf

cat $PGDATA/postgresql.conf
