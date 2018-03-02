

-- cleaning airport names for better matching;
CREATE OR REPLACE FUNCTION  geo_clean:mgrpid (geo_name text) 
    RETURNS text  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( regexp_replace(  nameclean( geo_name ) ,
 $$[[:<:]](:words2clean)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') );
$func$
;


drop table if exists                    :wd_input_table CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   :wd_input_table  as
select
    wd_id
    ,wd_label               as wd_name_en
    ,check_number(wd_label) as wd_name_has_num
    ,                 (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
    ,geo_clean:mgrpid((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
    ,iscebuano                                                         as wd_is_cebuano
    ,nSitelinks      
    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id 
    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array
    ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
    ,a_wof_type 
from wd.wdx 
where  :wd_filter   and  not iscebuano
;

CREATE INDEX  ON   :wd_input_table  USING GIST(wd_point_merc);
CREATE INDEX  ON   :wd_input_table (wd_id);
ANALYSE            :wd_input_table ;


--
---------------------------------------------------------------------------------------
--

drop table if exists          :ne_input_table CASCADE;
CREATE UNLOGGED TABLE         :ne_input_table as
select
     ogc_fid
    ,0::double precision as min_zoom    --  missing min_zoom      
    ,featurecla      
    ,name                as ne_name
    ,geo_clean:mgrpid(name)     as ne_una_name        
    ,check_number(name)  as ne_name_has_num
    ,ARRAY[name::text,geo_clean:mgrpid(name)::text,geo_clean:mgrpid(namealt)::text,unaccent(name)::text,unaccent(namealt)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(ST_Safe_Repair(geometry))   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point
    ,wikidataid as ne_wd_id
from ne.ne_10m_geography_regions_polys
where :ne_filter
;

CREATE INDEX  ON :ne_input_table  USING GIST(ne_geom_merc);
ANALYSE          :ne_input_table ;

\set _match             _match
\set _match_agg         _match_agg
\set _match_agg_sum     _match_agg_sum
\set _match_notfound    _match_notfound

\set ne_wd_match               newd.ne_wd_:mgrpid:_match
\set ne_wd_match_agg           newd.ne_wd_:mgrpid:_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_:mgrpid:_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_:mgrpid:_match_notfound

\set mcond1     (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2 and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance ))
\set mcond3

\ir 'template_newd_matching.sql'




