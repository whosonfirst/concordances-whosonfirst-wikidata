
-- cleaning airport names for better matching;
CREATE OR REPLACE FUNCTION  lake_clean(lake_name text) 
    RETURNS text  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( regexp_replace(  nameclean( lake_name ) ,
 $$[[:<:]](lake|lac|lagune|laguna|de|lago|lough|limni|see|di|ozero|reservoir|represa|presa|river|historic|ezers)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') );
$func$
;




CREATE OR REPLACE FUNCTION  lake_array_clean(arr1 text[],arr2 text[]) 
    RETURNS text[]  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
    select array_agg( distinct aname )  from 
    (
                 ( select  lake_clean( arr1name ) as aname from unnest(arr1) as arr1name )
      union all  ( select  lake_clean( arr2name ) as aname from unnest(arr1) as arr2name )
    ) t
$func$
;


drop table if exists    newd.wd_match_lake CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   newd.wd_match_lake  as
with x as
(
select
     wd_id
    ,wd_label               as wd_name_en
    ,check_number(wd_label) as wd_name_has_num
    ,           (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
    ,lake_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
    ,iscebuano                  as wd_is_cebuano
    ,nSitelinks
    --  ,get_wdc_value(data, 'P1566')      as p1566_geonames    
    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id 
    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array
    --  ,get_wd_concordances(data)         as wd_concordances_array
    ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
from wd.wdx 
where (a_wof_type  @> ARRAY['lake','hasP625'] ) and  not iscebuano
)
select *
      ,lake_array_clean(wd_name_array,wd_altname_array) as wd_all_name_array
from x      
;

CREATE INDEX  ON  newd.wd_match_lake USING GIST(wd_point_merc);
--CREATE INDEX  ON  newd.wd_match_lake (una_wd_name_en_clean);
CREATE INDEX  ON  newd.wd_match_lake (wd_id);
---CREATE INDEX  ON  newd.wd_match_lake USING GIN(wd_name_array );
--CREATE INDEX  ON  newd.wd_match_lake USING GIN(wd_altname_array );
ANALYSE   newd.wd_match_lake ;


--
---------------------------------------------------------------------------------------
--

drop table if exists          newd.ne_match_lake CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_lake  as
select
     ogc_fid
    ,min_zoom     
    ,featurecla 
    ,name                as ne_name
    ,lake_clean(name)    as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,lake_clean(name)::text,lake_clean(name_alt)::text,unaccent(name)::text,unaccent(name_alt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point
    ,'' as ne_wd_id
from ne.ne_10m_lakes
;

CREATE INDEX  ON newd.ne_match_lake  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_lake ;

\set wd_input_table           newd.wd_match_lake 
\set ne_input_table           newd.ne_match_lake

\set ne_wd_match               newd.ne_wd_match_lake_match
\set ne_wd_match_agg           newd.ne_wd_match_lake_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_lake_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_lake_match_notfound

\set safedistance   200000
\set searchdistance 400003
\set suggestiondistance  20000

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or( ne_name_array && wd_all_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2 and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance ))
\set mcond3

\ir 'template_newd_matching.sql'






--
---------------------------------------------------------------------------------------
--
--   no 'name_alt' column
drop table if exists          newd.ne_match_lake_europe CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_lake_europe  as
select
     ogc_fid
    ,min_zoom     
    ,featurecla 
    ,name                as ne_name
    ,lake_clean(name)    as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,lake_clean(name)::text,unaccent(name)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point
    ,'' as ne_wd_id
from ne.ne_10m_lakes_europe
;
CREATE INDEX  ON newd.ne_match_lake_europe  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_lake_europe ;

\set wd_input_table           newd.wd_match_lake 
\set ne_input_table           newd.ne_match_lake_europe

\set ne_wd_match               newd.ne_wd_match_lake_europe_match
\set ne_wd_match_agg           newd.ne_wd_match_lake_europe_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_lake_europe_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_lake_europe_match_notfound

\set safedistance           100000
\set searchdistance         400003
\set suggestiondistance      20000

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or ( ne_name_array && wd_all_name_array)  or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2 and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance ))
\set mcond3

\ir 'template_newd_matching.sql'





--
---------------------------------------------------------------------------------------
--

drop table if exists          newd.ne_match_lake_north_america CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_lake_north_america  as
select
     ogc_fid
    ,min_zoom     
    ,featurecla 
    ,name                as ne_name
    ,lake_clean(name)    as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,lake_clean(name)::text,lake_clean(name_alt)::text,unaccent(name)::text,unaccent(name_alt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point    
    ,'' as ne_wd_id
from ne.ne_10m_lakes_north_america
;
CREATE INDEX  ON newd.ne_match_lake_north_america  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_lake_north_america ;

\set wd_input_table           newd.wd_match_lake 
\set ne_input_table           newd.ne_match_lake_north_america

\set ne_wd_match               newd.ne_wd_match_lake_north_america_match
\set ne_wd_match_agg           newd.ne_wd_match_lake_north_america_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_lake_north_america_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_lake_north_america_match_notfound

\set safedistance   100000
\set searchdistance 400003
\set suggestiondistance  20000

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or( ne_name_array && wd_all_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2 and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance ))
\set mcond3

\ir 'template_newd_matching.sql'






--
---------------------------------------------------------------------------------------
--

drop table if exists          newd.ne_match_lake_historic CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_lake_historic  as
select
     ogc_fid
    ,min_zoom
    ,featurecla     
    ,name                as ne_name
    ,lake_clean(name)    as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,lake_clean(name)::text,lake_clean(name_alt)::text,unaccent(name)::text,unaccent(name_alt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point    
    ,'' as ne_wd_id
from ne.ne_10m_lakes_historic
;
CREATE INDEX  ON newd.ne_match_lake_historic  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_lake_historic ;

\set wd_input_table           newd.wd_match_lake 
\set ne_input_table           newd.ne_match_lake_historic

\set ne_wd_match               newd.ne_wd_match_lake_historic_match
\set ne_wd_match_agg           newd.ne_wd_match_lake_historic_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_lake_historic_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_lake_historic_match_notfound
\set safedistance   100000
\set searchdistance 400003

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or( ne_name_array && wd_all_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2 and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance ))
\set mcond3

\ir 'template_newd_matching.sql'
