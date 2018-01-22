
-- todo:  name_alt ->  
-- todo:  concordance matching
-- todo:  duplicate check

-- cleaning airport names for better matching;
CREATE OR REPLACE FUNCTION  adm1prov_clean(name text) 
    RETURNS text  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( regexp_replace(  nameclean( name ) ,
 $$[[:<:]](province)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') );
$func$
;


drop table if exists                    newd.wd_match_admin_1_states_provinces CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   newd.wd_match_admin_1_states_provinces  as
select
     wd_id
    ,wd_label               as wd_name_en
    ,check_number(wd_label) as wd_name_has_num
    ,          (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
    ,adm1prov_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
    ,iscebuano                  as wd_is_cebuano
    ,nSitelinks    
    --  ,get_wdc_value(data, 'P1566')      as p1566_geonames    
    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id 
    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array
    ,get_wd_concordances(data)         as wd_concordances_array
    ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
from wd.wdx 
where (a_wof_type  @> ARRAY['P300','hasP625'] )     and  not iscebuano
;

CREATE INDEX  ON  newd.wd_match_admin_1_states_provinces USING GIST(wd_point_merc);
CREATE INDEX  ON  newd.wd_match_admin_1_states_provinces (wd_id);
ANALYSE           newd.wd_match_admin_1_states_provinces ;


--
---------------------------------------------------------------------------------------
--

drop table if exists          newd.ne_match_admin_1_states_provinces CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_admin_1_states_provinces  as
select
     ogc_fid
    ,min_zoom      
    ,featurecla   
    ,name                as ne_name
    ,adm1prov_clean(name)    as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,adm1prov_clean(name)::text,adm1prov_clean(name_alt)::text,unaccent(name)::text,unaccent(name_alt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point    
    ,'' as ne_wd_id
from ne.ne_10m_admin_1_states_provinces
;

CREATE INDEX  ON newd.ne_match_admin_1_states_provinces  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_admin_1_states_provinces;

\set wd_input_table           newd.wd_match_admin_1_states_provinces
\set ne_input_table           newd.ne_match_admin_1_states_provinces

\set ne_wd_match               newd.ne_wd_match_admin_1_states_provinces_match
\set ne_wd_match_agg           newd.ne_wd_match_admin_1_states_provinces_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_admin_1_states_provinces_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_admin_1_states_provinces_match_notfound
\set safedistance   400000
\set searchdistance 800003
\set suggestiondistance  80000

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2  and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance )) 
\set mcond3

\ir 'template_newd_matching.sql'





