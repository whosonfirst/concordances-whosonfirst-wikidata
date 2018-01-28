#!/bin/bash

set -e
set -u

echo "======== parse wof for wikidataid ==========="

rm -f /wof/whosonfirst-data/wd.txt
find /wof/whosonfirst-data/data  -name *.geojson -exec  cat {} + | grep "wd:id" | cut -d'"' -f4 > /wof/whosonfirst-data/wd.txt
sort -u -o /wof/whosonfirst-data/wd.txt  /wof/whosonfirst-data/wd.txt
head /wof/whosonfirst-data/wd.txt
wc -l /wof/whosonfirst-data/wd.txt


echo "======== parse wikidataid_redirects ==========="

cat /wof/wikidata_dump/wikidata_redirects.csv | go run ./code/wdredirect_wofparse.go             > /wof/wikidata_dump/wikidata_redirects_filtered.csv
echo """
    -- import
    CREATE SCHEMA IF NOT EXISTS wd;
    DROP TABLE IF EXISTS wd.wd_redirects CASCADE;
    CREATE UNLOGGED TABLE wd.wd_redirects (wd_from text , wd_to text );
    \copy wd.wd_redirects (wd_from,wd_to)  FROM '/wof/wikidata_dump/wikidata_redirects_filtered.csv' DELIMITER ',' CSV HEADER ESCAPE '\"'
    --
""" | psql -e

cat /wof/wikidata_dump/wikidata_redirects_filtered.csv | cut -d',' -f2  | sed 's/$/,/' > /wof/whosonfirst-data/wd_redirects.csv
cat /wof/whosonfirst-data/wd.txt                                        | sed 's/$/,/' > /wof/whosonfirst-data/wd_extended.csv


echo "======== parse start: wikidata_dump/latest-all.json.gz ==========="
time go run /wof/code/wdpp.go /wof/wikidata_dump/latest-all.json.gz
echo "======== parse end: wikidata_dump/latest-all.json.gz ==========="

psql -c	"CREATE UNIQUE INDEX wd_wdx_wd_id       ON  wd.wdx(wd_id) 	   WITH (fillfactor = 100) ; " &
psql -c	"CREATE UNIQUE INDEX wdlabels_en_wd_id  ON  wdlabels.qlabel(wd_id) WITH (fillfactor = 100) ; " &
wait

psql -c	"CLUSTER   wdlabels.qlabel  USING  wdlabels_en_wd_id ; " &
psql -c	"CLUSTER   wd.wdx       USING  wd_wdx_wd_id      ; " &
wait

psql -c	"CREATE INDEX ON  wd.wdx USING GIN( a_wof_type ) ; " &
psql -c	"CREATE INDEX ON  wd.wdx USING GIST( geom )      ; " &
psql -c	"CREATE INDEX ON  wd.wdx(wd_id)  WITH (fillfactor = 100) ; " &
wait 

psql -c	"ALTER TABLE  wdlabels.qlabel  SET LOGGED  ; " &
psql -c	"ALTER TABLE  wd.wdx       SET LOGGED  ; " &
wait

psql -c	"ANALYSE wdlabels.qlabel;" &
psql -c	"ANALYSE wd.wdx     ;" &
wait 



echo """
    --
    SELECT a_wof_type, count(*) as N FROM wd.wdx GROUP BY a_wof_type;
    --
    SELECT count(*) as N_all FROM wd.wdx;
    --
    SELECT count(*) as N_no_geom FROM wd.wdx WHERE geom is null;
    --    
    \d+ wd.wdx
""" | psql -e

echo "-- end --"
