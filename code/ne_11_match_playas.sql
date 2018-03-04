
-- cleaning airport names for better matching;
CREATE OR REPLACE FUNCTION  playa_clean(geo_name text)
    RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( regexp_replace(  nameclean( geo_name ) ,
 $$[[:<:]](lake)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') );
$func$
;


drop table if exists                    newd.wd_match_playas CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   newd.wd_match_playas  as
select
     wd_id
    ,wd_label               as wd_name_en
    ,check_number(wd_label) as wd_name_has_num
    ,          (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
    ,playa_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
    ,iscebuano                  as wd_is_cebuano
    ,nSitelinks
    --  ,get_wdc_value(data, 'P1566')      as p1566_geonames
    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id
    ,get_wd_name_array(data)           as wd_name_array
    ,get_wd_altname_array(data)        as wd_altname_array
    --  ,get_wd_concordances(data)         as wd_concordances_array
    ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
    ,a_wof_type
from wd.wdx
where (a_wof_type  && ARRAY['playa','lake','depression'])
    and (a_wof_type  @>  ARRAY['hasP625'] )
    and not iscebuano
;

CREATE INDEX  ON  newd.wd_match_playas USING GIST(wd_point_merc);
CREATE INDEX  ON  newd.wd_match_playas (wd_id);
ANALYSE           newd.wd_match_playas ;


--
---------------------------------------------------------------------------------------
--
\set neextrafields   ,name_abb,name_alt,wdid_score


drop table if exists          newd.ne_match_playas CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_playas as
select
     ogc_fid
    ,min_zoom
    ,featurecla
    ,name                as ne_name
    ,playa_clean(name)   as ne_una_name
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,playa_clean(name)::text,playa_clean(name_alt)::text,unaccent(name)::text,unaccent(name_alt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point
    ,wikidataid as ne_wd_id
    :neextrafields 
from ne.ne_10m_playas
;

CREATE INDEX  ON newd.ne_match_playas  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_playas;

\set wd_input_table           newd.wd_match_playas
\set ne_input_table           newd.ne_match_playas

\set ne_wd_match               newd.ne_wd_match_playas_match
\set ne_wd_match_agg           newd.ne_wd_match_playas_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_playas_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_playas_match_notfound
\set safedistance   100000000
\set searchdistance 400000003
\set suggestiondistance  200000

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2
\set mcond3

\ir 'template_newd_matching.sql'




