#!/bin/bash

set -e
set -u

wof_repo=/wof/whosonfirst-data/
table=$1
csv=$2


echo """
    DROP TABLE IF EXISTS ${table} CASCADE ;
    CREATE TABLE ${table} (
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


echo "--------------- load ${table} - with wof-pgis-index -----------------"
time /wof/go-whosonfirst-pgis/bin/wof-pgis-index \
    -pgis-password $PGPASSWORD \
    -pgis-host     $PGHOST \
    -pgis-table ${table} \
    -mode meta ${wof_repo}meta/${csv}
    
echo "======== index & test: ${table} ==========="

echo """

    -- index --
    -- CREATE INDEX ${table}_by_geom        ON ${table} USING GIST(geom);
    -- CREATE INDEX ${table}_by_centroid    ON ${table} USING GIST(centroid);
    -- CREATE INDEX ${table}_by_placetype   ON ${table} (placetype_id);
    -- CREATE INDEX ${table}_by_properties  ON ${table} USING GIN (properties);
    -- CREATE INDEX ${table}_by_propertiesp ON ${table} USING GIN (properties jsonb_path_ops);

    -- analyze --
    analyze ${table};

    -- test --
    SELECT 
        id
    ,placetype_id
    ,properties->>'wof:name'                     AS wof_name
    ,properties->'wof:concordances'->>'wd:id'    AS wd_id
    FROM ${table}
    LIMIT 10;

    ;  
""" | psql


