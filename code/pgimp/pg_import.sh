#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

#  time ./code/pgimp/pg_import.sh hu


if [ -z "$1" ]
  then
    echo "Please: call with 1 argument:  wofadmin ( 2 letter iso ) "
    exit 1
fi

wadmin=$1

psql -e -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS tempadmin0_${wadmin} CASCADE;"
psql -e -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS tempadmin1_${wadmin} CASCADE;"
psql -e -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS tempadmin2_${wadmin} CASCADE;"
psql -e -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS tempadmin3_${wadmin} CASCADE;"
psql -e -v ON_ERROR_STOP=1 -c "CREATE UNLOGGED TABLE tempadmin0_${wadmin} ( data jsonb NOT NULL );"
psql -e -v ON_ERROR_STOP=1 -c "CREATE UNLOGGED TABLE tempadmin1_${wadmin} ( data jsonb NOT NULL );"
psql -e -v ON_ERROR_STOP=1 -c "CREATE UNLOGGED TABLE tempadmin2_${wadmin} ( data jsonb NOT NULL );"
psql -e -v ON_ERROR_STOP=1 -c "CREATE UNLOGGED TABLE tempadmin3_${wadmin} ( data jsonb NOT NULL );"

cd /wof/whosonfirst-data/whosonfirst-data-admin-${wadmin}
find ./data -name '*.geojson' -print  | grep -v alt > j.csv
cat j.csv | wc -l

split -x  --number=l/4 j.csv j.csv    # -->  j.csv00  j.csv01  j.csv02  j.csv03

echo "... start import 4 threads (${wadmin}) .."
cat j.csv00 | xargs -I {} sh -c "paste -s -d ' ' {} " | psql -e -v ON_ERROR_STOP=1 -c "COPY tempadmin0_${wadmin} (data) FROM STDIN csv quote e'\x01' delimiter e'\x02' ;" &
cat j.csv01 | xargs -I {} sh -c "paste -s -d ' ' {} " | psql -e -v ON_ERROR_STOP=1 -c "COPY tempadmin1_${wadmin} (data) FROM STDIN csv quote e'\x01' delimiter e'\x02' ;" &
cat j.csv02 | xargs -I {} sh -c "paste -s -d ' ' {} " | psql -e -v ON_ERROR_STOP=1 -c "COPY tempadmin2_${wadmin} (data) FROM STDIN csv quote e'\x01' delimiter e'\x02' ;" &
cat j.csv03 | xargs -I {} sh -c "paste -s -d ' ' {} " | psql -e -v ON_ERROR_STOP=1 -c "COPY tempadmin3_${wadmin} (data) FROM STDIN csv quote e'\x01' delimiter e'\x02' ;" &
wait
echo "....end of import 4 threads (${wadmin}) | unlogged tables loaded! "

# jq -r -c .  ./data/859/070/35/85907035.geojson

echo """
CREATE SCHEMA IF NOT EXISTS wofadmin;
CREATE TABLE  IF NOT EXISTS wofadmin.wofdata (
     id             BIGINT  NOT NULL
    ,repo           CHAR(2) NOT NULL COLLATE \"C\"
    ,wof_name       TEXT
    ,parent_id      BIGINT
    ,placetype      TEXT NOT NULL COLLATE \"C\"
    ,wd_id          TEXT          COLLATE \"C\"
    ,is_superseded  SMALLINT NOT NULL -- properties.wof:superseded_by
    ,is_deprecated  SMALLINT NOT NULL -- properties.edtf:deprecated
    ,properties     JSONB NOT NULL
    ,lastmod        TIMESTAMP NOT NULL
    ,geom           GEOMETRY(Geometry, 4326)
    ,centroid       GEOMETRY(POINT, 4326) NOT NULL
)
PARTITION BY LIST(repo)
;

DROP TABLE IF EXISTS wofadmin.wofdata_${wadmin} CASCADE ;
CREATE TABLE wofadmin.wofdata_${wadmin}
    PARTITION OF wofadmin.wofdata
      FOR VALUES in ('${wadmin}')
        WITH (fillfactor = 100)
;
INSERT INTO wofadmin.wofdata_${wadmin} (
     id
    ,repo
    ,wof_name
    ,parent_id
    ,placetype
    ,wd_id
    ,is_superseded
    ,is_deprecated
    ,properties
    ,lastmod
    ,geom
    ,centroid
)
SELECT *

FROM
(
  SELECT
   (data->'id')::bigint                                     as id
  ,'${wadmin}'::CHAR(2)                                     as repo
  ,(data->'properties'->>'wof:name')                        as wof_name
  ,(data->'properties'->'wof:parent_id')::bigint            as parent_id
  ,(data->'properties'->>'wof:placetype')::text             as placetype
  ,(data->'properties'->'wof:concordances'->>'wd:id')::text as wd_id
  ,CASE
      WHEN (data->'properties'->'wof:superseded_by')::text = '[]'  THEN 0
      WHEN (data->'properties'->'wof:superseded_by')::text is null THEN 0
      ELSE 1
    END   as is_superseded
  ,CASE
      WHEN (data->'properties'->'edtf:deprecated')::text is NULL   THEN  0
      WHEN (data->'properties'->'edtf:deprecated')::text = 'u'     THEN -1
      WHEN (data->'properties'->'edtf:deprecated')::text = 'uuuu'  THEN -1
      ELSE 1
    END   as is_deprecated
  ,(data->'properties')::jsonb                                      as properties
  ,to_timestamp( (data->'properties'->'wof:lastmodified')::bigint)  as lastmod
  ,(ST_Force2D(ST_GeomFromGeoJSON(data->'geometry')))::GEOMETRY     as geom
  ,coalesce
  (
    ST_SetSRID(ST_MakePoint(
        (data->'properties'->'lbl:longitude')::float,
        (data->'properties'->'lbl:latitude')::float
      ),4326)  --  as lbl_centroid
    ,ST_SetSRID(ST_MakePoint(
        (data->'properties'->'reversegeo:longitude')::float,
        (data->'properties'->'reversegeo::latitude')::float
      ),4326)  --  as reversegeo_centroid
    ,ST_SetSRID(ST_MakePoint(
        (data->'properties'->'geom:longitude')::float,
        (data->'properties'->'geom:latitude')::float
      ),4326)  -- as geom_centroid
  )::GEOMETRY(POINT,4326) as centroid
  FROM (
                   select data from tempadmin0_${wadmin}
         union all select data from tempadmin1_${wadmin}
         union all select data from tempadmin2_${wadmin}
         union all select data from tempadmin3_${wadmin}
        ) x
  ORDER BY 1
) as t
-- keep only the valid values !
WHERE (is_superseded=0 and is_deprecated=0 )
;

DROP TABLE IF EXISTS tempadmin0_${wadmin} ;
DROP TABLE IF EXISTS tempadmin1_${wadmin} ;
DROP TABLE IF EXISTS tempadmin2_${wadmin} ;
DROP TABLE IF EXISTS tempadmin3_${wadmin} ;

CREATE UNIQUE INDEX wofdata_${wadmin}_id
    ON wofadmin.wofdata_${wadmin} USING BTREE (id)
      WITH (fillfactor = 100);

CLUSTER VERBOSE wofadmin.wofdata_${wadmin} USING wofdata_${wadmin}_id;

CREATE INDEX wofdata_${wadmin}_gix1
    ON wofadmin.wofdata_${wadmin} USING GIST (geom)
      WITH (fillfactor = 100);
CREATE INDEX wofdata_${wadmin}_gix2
    ON wofadmin.wofdata_${wadmin} USING GIST (centroid)
      WITH (fillfactor = 100);
CREATE INDEX wofdata_${wadmin}_wdidx
    ON wofadmin.wofdata_${wadmin}(wd_id)
      WITH (fillfactor = 100);
CREATE INDEX wofdata_${wadmin}_namex
    ON wofadmin.wofdata_${wadmin}(wof_name)
      WITH (fillfactor = 100);
CREATE INDEX wofdata_${wadmin}_placetypex
    ON wofadmin.wofdata_${wadmin}(placetype)
      WITH (fillfactor = 100);
-- CREATE INDEX wofdata_${wadmin}_propertiesg
--    ON wofadmin.wofdata_${wadmin} USING GIN(properties)
--    ;

ANALYZE wofadmin.wofdata_${wadmin};

\dti+ wofadmin.wofdata_${wadmin}*
\d+ wofadmin.wofdata_${wadmin}

""" | psql -e -v ON_ERROR_STOP=1
