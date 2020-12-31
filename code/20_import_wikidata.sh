#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

echo "======== parse wof for wikidataid ==========="

rm -f /wof/whosonfirst-data/wd.txt
time find /wof/whosonfirst-data/whosonfirst-data-admin-??/data/*  -name *.geojson -exec  cat {} + | grep "wd:id" | cut -d'"' -f4 > /wof/whosonfirst-data/wd.txt

# select duplicated wd_id
#sort | uniq -d /wof/whosonfirst-data/wd.txt > /wof/whosonfirst-data/wd_wofdups.txt

sort -u -o /wof/whosonfirst-data/wd.txt  /wof/whosonfirst-data/wd.txt
head /wof/whosonfirst-data/wd.txt
wc -l /wof/whosonfirst-data/wd.txt


echo "======== parse wikidataid_redirects ==========="

cat /wof/wikidata_dump/wikidata_redirects.csv | go run /wof/code/wdredirect_wofparse.go             > /wof/wikidata_dump/wikidata_redirects_filtered.csv
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


psql -c	"CREATE UNIQUE INDEX wd_wdok_wd_id         ON  wd.wd_ok(wd_id)          INCLUDE ( a_wof_type)  WITH (fillfactor = 100) ; " 
psql -c	"CREATE UNIQUE INDEX wd_wdnogeom1_wd_id    ON  wd.wd_nogeom_grp1(wd_id) INCLUDE ( a_wof_type)  WITH (fillfactor = 100) ; " 
psql -c	"CREATE UNIQUE INDEX wd_wdnogeom2_wd_id    ON  wd.wd_nogeom_grp2(wd_id) INCLUDE ( a_wof_type)  WITH (fillfactor = 100) ; " 
psql -c	"CREATE UNIQUE INDEX wd_wdmessy_wd_id      ON  wd.wd_messy(wd_id)       INCLUDE ( a_wof_type)  WITH (fillfactor = 100) ; " 
psql -c	"CREATE UNIQUE INDEX wd_wddemolished_wd_id ON  wd.wd_demolished(wd_id)  INCLUDE ( a_wof_type)  WITH (fillfactor = 100) ; " 

psql -c	"CREATE UNIQUE INDEX wdlabels_en_wd_id   ON  wdlabels.qlabel(wd_id) WITH (fillfactor = 100) ; " 


#psql -c	"CLUSTER   wdlabels.qlabel  USING  wdlabels_en_wd_id ; " &
#psql -c	"CLUSTER   wd.wd_ok         USING  wd_wdok_wd_id     ; " &
#wait

psql -c	"CREATE INDEX ON  wd.wd_ok USING GIN( a_wof_type ) WITH (fastupdate = off); " 


psql -c	"CREATE INDEX ON  wd.wd_ok USING GIST( geom )      ; " &
psql -c	"CREATE INDEX ON  wd.wd_ok(wd_id)  WITH (fillfactor = 100) ; " &
wait 

psql -c	"ALTER TABLE  wdlabels.qlabel  SET LOGGED  ; " &
psql -c	"ALTER TABLE  wd.wd_ok         SET LOGGED  ; " &
wait

psql -c	"ALTER TABLE  wd.wd_nogeom_grp1   SET LOGGED  ; " &
psql -c	"ALTER TABLE  wd.wd_nogeom_grp2   SET LOGGED  ; " &
psql -c	"ALTER TABLE  wd.wd_messy         SET LOGGED  ; " &
psql -c	"ALTER TABLE  wd.wd_demolished    SET LOGGED  ; " &
wait

psql -c	"ANALYSE  wd.wd_nogeom_grp1   ; " &
psql -c	"ANALYSE  wd.wd_nogeom_grp2   ; " &
psql -c	"ANALYSE  wd.wd_messy         ; " &
psql -c	"ANALYSE  wd.wd_demolished    ; " &
wait

psql -c	"CREATE INDEX ON wd.wd_nogeom_grp1 USING GIN( a_wof_type ) WITH (fastupdate = off); " &
psql -c	"CREATE INDEX ON wd.wd_nogeom_grp2 USING GIN( a_wof_type ) WITH (fastupdate = off); " &
psql -c	"CREATE INDEX ON wd.wd_messy       USING GIN( a_wof_type ) WITH (fastupdate = off); " &
psql -c	"CREATE INDEX ON wd.wd_demolished  USING GIN( a_wof_type ) WITH (fastupdate = off); " &
wait

psql -c	"ANALYSE wdlabels.qlabel;" &
psql -c	"ANALYSE wd.wd_ok       ;" &
wait 



echo """
--

--    
\d+ wd.wd_ok
--
DROP TABLE IF EXISTS wd.wdx CASCADE;
DROP VIEW  IF EXISTS wd.wdx CASCADE;
CREATE VIEW  wd.wdx as
            select * from wd.wd_ok
 union all  select * from wd.wd_nogeom_grp1
 union all  select * from wd.wd_nogeom_grp2 
 union all  select * from wd.wd_messy
 ;

""" | psql -e


echo "-- end --"
