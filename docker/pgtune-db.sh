#!/bin/bash
set -o errexit


nproc=$(nproc)
db_connection=$(( $nproc + 6 ))

python ./pgtune.py  -c ${db_connection}  > $PGDATA/postgresql.conf

cat $PGDATA/postgresql.conf
