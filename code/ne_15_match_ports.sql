



CREATE OR REPLACE FUNCTION  ports_clean(name text) 
    RETURNS text  
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE   AS
$func$
select trim( translate( regexp_replace(  nameclean( name ) ,
$$[[:<:]](port|harbour|harbor|port|porto|puerto|seaport|hafen|havn|of|di|de|du|do|)[[:>:]]$$,
  ' ',
  'gi'
),'  ',' ') );
$func$
;


drop table if exists                    newd.wd_match_ports CASCADE;
EXPLAIN ANALYSE CREATE UNLOGGED TABLE   newd.wd_match_ports  as
select
     wd_id
    ,wd_label               as wd_name_en
    ,check_number(wd_label) as wd_name_has_num
    ,            (regexp_split_to_array( wd_label , '[,()]'))[1]  as wd_name_en_clean
    ,ports_clean((regexp_split_to_array( wd_label , '[,()]'))[1]) as una_wd_name_en_clean
    ,iscebuano                  as wd_is_cebuano
    ,nSitelinks    
    --  ,get_wdc_value(data, 'P1566')      as p1566_geonames    
    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id 
    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array
    ,get_wd_concordances(data)         as wd_concordances_array
    ,cartodb.CDB_TransformToWebmercator(geom::geometry)  as wd_point_merc
    ,(get_wdc_value(data, 'P238'))->>0 as wd_iata
    ,(get_wdc_value(data, 'P239'))->>0 as wd_icao
from wd.wdx 
where   (a_wof_type  && ARRAY['port','P1624']) 
    and (a_wof_type  @> ARRAY['hasP625'] )
    and not iscebuano
 --   and geom &&  ST_MakeEnvelope(-112.280,15.547,-92.549,25.721 ,4326)
;

CREATE INDEX  ON  newd.wd_match_ports USING GIST(wd_point_merc);
CREATE INDEX  ON  newd.wd_match_ports (wd_id);
ANALYSE           newd.wd_match_ports ;


--
---------------------------------------------------------------------------------------
--
\set neextrafields   ,scalerank,website,natlscale 

drop table if exists          newd.ne_match_ports CASCADE;
CREATE UNLOGGED TABLE         newd.ne_match_ports  as
select
     ogc_fid    
    ,0::double precision as min_zoom   
    ,featurecla   
    ,name                 as ne_name
    ,ports_clean(name)    as ne_una_name        
    ,check_number(name)   as ne_name_has_num
    ,ARRAY[name::text,ports_clean(name)::text,unaccent(name)::text]     as ne_name_array
    ,cartodb.CDB_TransformToWebmercator(geometry)   as ne_geom_merc
    ,ST_PointOnSurface(geometry)  as ne_point    
    ,'' as ne_wd_id
    :neextrafields
from ne.ne_10m_ports
--where geometry &&  ST_MakeEnvelope(-112.280,15.547,-92.549,25.721 ,4326)
;

CREATE INDEX  ON newd.ne_match_ports  USING GIST(ne_geom_merc);
ANALYSE          newd.ne_match_ports;

\set wd_input_table           newd.wd_match_ports
\set ne_input_table           newd.ne_match_ports

\set ne_wd_match               newd.ne_wd_match_ports_match
\set ne_wd_match_agg           newd.ne_wd_match_ports_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_ports_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_ports_match_notfound
\set safedistance   200000
\set searchdistance 400003
\set suggestiondistance  400000

\set mcond1   (( ne.ne_una_name = wd.una_wd_name_en_clean ) or (  wd_name_array && ne_name_array ) or (  ne_name_array && wd_altname_array )  or (jarowinkler( ne.ne_una_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond2  and (ST_DWithin ( wd.wd_point_merc, ne.ne_geom_merc , :searchdistance )) 
\set mcond3

\ir 'template_newd_matching.sql'





