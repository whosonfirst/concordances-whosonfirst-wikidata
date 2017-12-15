



drop table if exists  wfwd.wd_match_region CASCADE;
create table          wfwd.wd_match_region  as
    select
     data->>'id'::text                  as wd_id  
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en_clean
    ,nameclean(get_wdlabeltext(data->>'id'::text))   as una_wd_name_en_clean

    ,get_countrycode( (get_wdc_item(data,'P17'))->>0 )   as wd_country 

    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id    

    --,(get_wdc_value(data, 'P901'))->>0  as fips10_4
    --,(get_wdc_value(data, 'P297'))->>0  as country_iso2

    --,get_wdc_value(data, 'P300')    as p300_iso3166_2
    --,get_wdc_value(data, 'P901')    as p901_fips10_4
    --,get_wdc_value(data, 'P1566')   as p1566_geonames
        
    --,get_wdc_monolingualtext(data, 'P1813')   as p1813_short_name
    --,get_wdc_monolingualtext(data, 'P1549')   as p1549_demonym
    --,get_wdc_monolingualtext(data, 'P1448')   as p1448_official_name
    --,get_wdc_monolingualtext(data, 'P1705')   as p1705_native_label
    --,get_wdc_monolingualtext(data, 'P1449')   as p1449_nick_name    
    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array
    ,get_wd_concordances(data)         as wd_concordances_array

    ,CDB_TransformToWebmercator(ST_SetSRID(ST_MakePoint( 
             cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
            )
    , 4326)) as wd_point_merc
    
    from wd.wdx as wd
    where a_wof_type && ARRAY['region'] 
    order by wd_country , una_wd_name_en_clean
    ;
    
;

CREATE INDEX  ON wfwd.wd_match_region (una_wd_name_en_clean);
CREATE INDEX  ON wfwd.wd_match_region (wd_id);
CREATE INDEX  ON wfwd.wd_match_region USING GIN(wd_name_array );
CREATE INDEX  ON wfwd.wd_match_region USING GIN(wd_altname_array );
ANALYSE   wfwd.wd_match_region;




drop table if exists wfwd.wof_match_region CASCADE;
create table         wfwd.wof_match_region  as
select
     wof.id
    ,wof.properties->>'wof:name'            as wof_name 
    ,nameclean(wof.properties->>'wof:name')  as una_wof_name 
    ,wof.properties->>'wof:country'         as wof_country
    ,wof.wd_id                              as wof_wd_id
    ,get_wof_name_array(wof.properties)     as wof_name_array
    ,get_wof_concordances(wof.properties)   as wof_concordances_array
    ,CDB_TransformToWebmercator(COALESCE( wof.geom::geometry, wof.centroid::geometry ))  as wof_geom_merc
from wf.wof_region as wof
where  wof.is_superseded=0 
   and wof.is_deprecated=0
order by wof_country ,  una_wof_name  
;

CREATE INDEX  ON wfwd.wof_match_region (wof_country);
CREATE INDEX  ON wfwd.wof_match_region USING GIN(wof_name_array );
ANALYSE  wfwd.wof_match_region ;



\set searchdistance      .
\set safedistance   1000000
\set wd_input_table           wfwd.wd_match_region
\set wof_input_table          wfwd.wof_match_region

\set wd_wof_match             wfwd.wd_mregion_wof_match
\set wd_wof_match_agg         wfwd.wd_mregion_wof_match_agg
\set wd_wof_match_agg_sum     wfwd.wd_mregion_wof_match_agg_summary
\set wd_wof_match_notfound    wfwd.wd_mregion_wof_match_notfound

\set mcond1      ( wof.wof_country  = wd.wd_country           )
\set mcond2  and (( wof.una_wof_name = wd.una_wd_name_en_clean ) or (wof_name_array && wd_name_array ) or (  wof_name_array && wd_altname_array ) or (wd_concordances_array && wof_concordances_array) or (jarowinkler(wof.una_wof_name, wd.una_wd_name_en_clean)>.971 ) )
\set mcond3  


\ir 'template_matching.sql'


