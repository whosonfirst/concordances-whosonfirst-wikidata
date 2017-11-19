#!/bin/bash

set -e
set -u

part=$1


echo "START:======== load wdpalce ${part}  ==========="

echo """
    --
    CREATE SCHEMA IF NOT EXISTS wdplace;
    DROP TABLE IF EXISTS wdplace.wd${part} CASCADE;
    --
""" | psql -e

time pgfutter --schema wdplace --table wd${part} --jsonb  json /wof/wikidata_dump/wdplace0${part}.json

echo """
    --
    -- CREATE INDEX wdplace_wd${part}_gin  ON wdplace.wd${part} USING GIN (data);
    --
    ANALYZE wdplace.wd${part};
    --
    SELECT count(*) FROM wdplace.wd${part} ;
    --
    \d+ wdplace.wd${part} 
    --
""" | psql -e


echo "END:======== load wdpalce ${part} ==========="
