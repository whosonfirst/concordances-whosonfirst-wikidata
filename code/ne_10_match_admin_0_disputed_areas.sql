
-- todo:  name_alt ->  
-- todo:  concordance matching
-- todo:  duplicate check

-- cleaning airport names for better matching;
CREATE OR REPLACE FUNCTION  adm0disputed_clean(name text) 
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


drop table if exists                    newd.wd_match_admin_0_disputed_areas CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   newd.wd_match_admin_0_disputed_areas  as
with x as
(
    select
         wd_id
        ,wd_label               as wd_name_en
        ,check_number(wd_label) as wd_name_has_num
        ,          (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
        ,adm0disputed_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
        ,is_cebuano(data)                  as wd_is_cebuano
      --  ,get_wdc_value(data, 'P1566')      as p1566_geonames    
        ,get_wdc_item_label(data,'P31')    as p31_instance_of
        ,get_wdc_item_label(data,'P17')    as p17_country_id 
        ,get_wd_name_array(data)           as wd_name_array 
        ,get_wd_altname_array(data)        as wd_altname_array
        ,get_wd_concordances(data)         as wd_concordances_array
        ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
        ,a_wof_type
    from wd.wdx 
    where (a_wof_type  &&  ARRAY['disputed','P1310','P1336']   )   and ( a_wof_type  @> ARRAY['hasP625'] ) 
)
select * 
from x
where (not wd_is_cebuano )   ;


CREATE INDEX  ON  newd.wd_match_admin_0_disputed_areas USING GIST(wd_point_merc);
CREATE INDEX  ON  newd.wd_match_admin_0_disputed_areas (wd_id);
ANALYSE           newd.wd_match_admin_0_disputed_areas ;


--
---------------------------------------------------------------------------------------
--

drop table if exists          newd.ne_match_admin_0_disputed_areas CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_admin_0_disputed_areas  as
select
     ogc_fid
    ,brk_name                        as ne_name
    ,adm0disputed_clean(brk_name)    as ne_una_name        
    ,check_number(brk_name)          as ne_name_has_num
    ,ARRAY[brk_name::text,adm0disputed_clean(brk_name)::text,adm0disputed_clean(name_alt)::text,unaccent(brk_name)::text,unaccent(name_alt)::text,unaccent(name_sort)::text,name_sort::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,'' as ne_wd_id
from ne.ne_10m_admin_0_disputed_areas
;

CREATE INDEX  ON newd.ne_match_admin_0_disputed_areas  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_admin_0_disputed_areas;

\set wd_input_table           newd.wd_match_admin_0_disputed_areas
\set ne_input_table           newd.ne_match_admin_0_disputed_areas

\set ne_wd_match               newd.ne_wd_match_admin_0_disputed_areas_match
\set ne_wd_match_agg           newd.ne_wd_match_admin_0_disputed_areas_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_admin_0_disputed_areas_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_admin_0_disputed_areas_match_notfound
\set safedistance   400000
\set searchdistance 800003

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2  and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance )) 
\set mcond3

\ir 'template_newd_matching.sql'





