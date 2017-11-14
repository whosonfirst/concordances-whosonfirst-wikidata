#!/bin/bash

set -e
set -u


echo "START:======== parse wikidata_dump/latest-all.json.gz   by wdplaceparse.go ==========="
ls -la /wof/wikidata_dump/latest-all.json.*
rm -f /wof/wikidata_dump/wdplace*

go run ./code/wdplaceparse.go | split -d --additional-suffix=.json  -n r/8 - /wof/wikidata_dump/wdplace

echo """
    --
    CREATE SCHEMA IF NOT EXISTS wdplace;
    DROP TABLE IF EXISTS wdplace.wd0 CASCADE;
    DROP TABLE IF EXISTS wdplace.wd1 CASCADE;
    DROP TABLE IF EXISTS wdplace.wd2 CASCADE;
    DROP TABLE IF EXISTS wdplace.wd3 CASCADE;            
    --
""" | psql


time pgfutter --schema wdplace --table wd0 --jsonb  json /wof/wikidata_dump/wdplace00.json
time pgfutter --schema wdplace --table wd1 --jsonb  json /wof/wikidata_dump/wdplace01.json
time pgfutter --schema wdplace --table wd2 --jsonb  json /wof/wikidata_dump/wdplace02.json
time pgfutter --schema wdplace --table wd3 --jsonb  json /wof/wikidata_dump/wdplace03.json



echo """
    --
    CREATE INDEX wdplace_wd0_gin  ON wdplace.wd0 USING GIN (data);
    CREATE INDEX wdplace_wd1_gin  ON wdplace.wd1 USING GIN (data);
    CREATE INDEX wdplace_wd2_gin  ON wdplace.wd2 USING GIN (data);
    CREATE INDEX wdplace_wd3_gin  ON wdplace.wd3 USING GIN (data);        
    --

    ANALYZE wdplace.wd0;
    ANALYZE wdplace.wd1;
    ANALYZE wdplace.wd2;
    ANALYZE wdplace.wd3;          
    --
    create or replace view      wdplace.wd AS
                  select * from wdplace.wd0         
        union all select * from wdplace.wd1  
        union all select * from wdplace.wd2  
        union all select * from wdplace.wd3  
    ;

    SELECT count(*) FROM wdplace.wd ;
    --
    \d+ wdplace.wd 
""" | psql


echo "END:======== parse wikidata_dump/latest-all.json.gz   by wdplaceparse.go ==========="

