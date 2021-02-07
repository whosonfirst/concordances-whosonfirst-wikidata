#!/bin/bash
set -o errexit

nproc=$(nproc)
db_connection=$(( $nproc * 4 + 20))
echo " nproc= ${nproc} "
echo " db_connection= ${db_connection} "
python3 ./pgtune.py  -c ${db_connection}  > $PGDATA/postgresql.conf

cat $PGDATA/postgresql.conf
