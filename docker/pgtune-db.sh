#!/bin/bash
set -o errexit


nproc=$(nproc)
db_connection=$(( $nproc + 10 ))

python ./pgtune.py -S -c ${db_connection}  > $PGDATA/postgresql.conf

cat $PGDATA/postgresql.conf
