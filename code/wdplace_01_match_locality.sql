

--  TODO: get the latest  P31 status.
--  Q1018914  | Q1018914   has an english name ( has an english label)
--  wikidata has a coordinate
--  TODO: wikidata has any of enwiki, eswiki, dewiki, ptwiki, ruwiki, znwiki page  [  so not a cebuano import ] 
--  distance < 100km
--  only 1 match!

drop table if exists  wdplace.wd_match_locality CASCADE;
create table          wdplace.wd_match_locality  as
with x AS (
                  select * from wdplace.wd0_x         
        union all select * from wdplace.wd1_x  
        union all select * from wdplace.wd2_x  
        union all select * from wdplace.wd3_x
        union all select * from wdplace.wd4_x  
        union all select * from wdplace.wd5_x  
        union all select * from wdplace.wd6_x
        union all select * from wdplace.wd7_x
    )
    SELECT * 
          , unaccent(wd_name_en_clean) as una_wd_name_en_clean  
    FROM x 
    WHERE wd_id != wd_name_en
      and wd_point is not null 
      and wd_is_cebuano IS FALSE
    ;

CREATE INDEX  ON  wdplace.wd_match_locality USING GIST(wd_point);
CREATE INDEX  ON  wdplace.wd_match_locality (una_wd_name_en_clean);
CREATE INDEX  ON  wdplace.wd_match_locality (wd_id);
CREATE INDEX  ON  wdplace.wd_match_locality USING GIN(wd_name_array );
CREATE INDEX  ON  wdplace.wd_match_locality USING GIN(wd_altname_array );
ANALYSE   wdplace.wd_match_locality ;




drop table if exists wof_match_locality CASCADE;
create table         wof_match_locality  as
select
     wof.id
    ,wof.properties->>'wof:name'            as wof_name 
    ,unaccent(wof.properties->>'wof:name')  as una_wof_name 
    ,wof.properties->>'wof:country'         as wof_country
    ,wof.wd_id                              as wof_wd_id
    ,get_wof_name_array(wof.properties)     as wof_name_array
    ,COALESCE( wof.geom::geometry, wof.centroid::geometry )  as wof_geom
from wof_locality as wof
where  wof.is_superseded=0 
   and wof.is_deprecated=0
;

CREATE INDEX  ON wof_match_locality  USING GIST(wof_geom);
CREATE INDEX  ON wof_match_locality   (una_wof_name);
CREATE INDEX  ON wof_match_locality  USING GIN ( wof_name_array);
ANALYSE          wof_match_locality ;


--
---------------------------------------------------------------------------------------
--

\set wd_input_table           wdplace.wd_match_locality
\set wof_input_table          wof_match_locality

\set wd_wof_match             wd_mlocality_wof_match
\set wd_wof_match_agg         wd_mlocality_wof_match_agg
\set wd_wof_match_agg_sum     wd_mlocality_wof_match_agg_summary
\set wd_wof_match_notfound    wd_mlocality_wof_match_notfound

\set mcond1     (( wof.una_wof_name = wd.una_wd_name_en_clean ) or (wof_name_array && wd_name_array ) or (  wof_name_array && wd_altname_array ) or (jarowinkler(wof.una_wof_name, wd.una_wd_name_en_clean)>.901 ) )
--  \set mcond1     (( wof.una_wof_name = wd.una_wd_name_en_clean ) )
\set mcond2  and (ST_Distance(CDB_TransformToWebmercator(wd.wd_point),CDB_TransformToWebmercator(wof.wof_geom) )::bigint  <= 100001 )
\set mcond3  

\set safedistance 40000

\ir 'template_matching.sql'

