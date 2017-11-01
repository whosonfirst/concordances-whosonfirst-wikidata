#!/bin/bash

set -e
set -u

echo """
    --
    CREATE SCHEMA IF NOT EXISTS  wikidata;
    DROP TABLE IF EXISTS wikidata.wd;
    --
""" | psql


pgfutter --schema wikidata \
         --table wd \
         --jsonb \
         json wd.json

echo """
    --
    CREATE INDEX wikidata_wd_jsonb  ON wikidata.wd USING GIN (data);
    CREATE INDEX wikidata_wd_jsonbp ON wikidata.wd USING GIN (data jsonb_path_ops);
    --
    ANALYZE wikidata.wd;
    --
    SELECT count(*) FROM wikidata.wd ;
    --
    \d+ wikidata.wd 
""" | psql
