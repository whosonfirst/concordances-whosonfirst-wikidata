#!/bin/bash

set -e
set -u
echo "======== 02_load_wof START ==========="

echo """
    DROP TABLE IF EXISTS whosonfirst;
    CREATE TABLE whosonfirst (
         id             BIGINT PRIMARY KEY
        ,parent_id      BIGINT
        ,placetype_id   BIGINT
        ,is_superseded  SMALLINT
        ,is_deprecated  SMALLINT
        ,meta           JSONB
        ,properties     JSONB
        ,geom_hash      CHAR(32)
        ,lastmod        CHAR(25)
        ,geom           GEOGRAPHY(MULTIPOLYGON, 4326)
        ,centroid       GEOGRAPHY(POINT, 4326)
    )
    ;  
""" | psql

awk -F, 'NR==1 || ( $15=="HU"  ) ' \
     /wof/whosonfirst-data/meta/wof-locality-latest.csv > /wof/whosonfirst-data/meta/xx-wof-locality-latest.csv

#cp ../whosonfirst-data/meta/wof-locality-latest.csv ../whosonfirst-data/meta/wof-xx-locality-latest.csv

echo "--------------- load with wof-pgis-index -----------------"
time /wof/go-whosonfirst-pgis/bin/wof-pgis-index \
     -pgis-password $PGPASSWORD \
     -pgis-host     $PGHOST \
     -mode meta /wof/whosonfirst-data/meta/xx-wof-locality-latest.csv

#    -verbose \

echo "======== index & test ==========="

echo """

    -- index --
    CREATE INDEX by_geom        ON whosonfirst USING GIST(geom);
    CREATE INDEX by_centroid    ON whosonfirst USING GIST(centroid);
    CREATE INDEX by_placetype   ON whosonfirst (placetype_id);
    CREATE INDEX by_properties  ON whosonfirst USING GIN (properties);
    CREATE INDEX by_propertiesp ON whosonfirst USING GIN (properties jsonb_path_ops);

    -- analyze --
    analyze whosonfirst;

    -- test --
    SELECT 
        id
       ,placetype_id
       ,properties->>'wof:name'                     AS wof_name
       ,properties->'wof:concordances'->>'wd:id'    AS wd_id
    FROM whosonfirst 
    LIMIT 10;

    ;  
""" | psql

echo "======== 02_load_wof END ==========="

# psql -f ./wof_sql_step1.sql 
