



drop table if exists  wdplace.wd_match_country CASCADE;
create table          wdplace.wd_match_country  as
with c as
(
    select
     data->>'id'::text                  as wd_id
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en_clean
    ,unaccent(get_wdlabeltext(data->>'id'::text))   as una_wd_name_en_clean

    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id

    ,(get_wdc_value(data, 'P901'))->>0  as fips10_4
    ,(get_wdc_value(data, 'P297'))->>0  as country_iso2

    ,get_wdc_value(data, 'P297')    as p297_iso3166_1_alpha2
    ,get_wdc_value(data, 'P298')    as p298_iso3166_1_alpha3
    ,get_wdc_value(data, 'P299')    as p299_iso3166_1_numeric

    ,get_wdc_value(data, 'P300')    as p300_iso3166_2
    ,get_wdc_value(data, 'P901')    as p901_fips10_4
    ,get_wdc_value(data, 'P1566')   as p1566_geonames

    ,get_wdc_monolingualtext(data, 'P1813')   as p1813_short_name
    ,get_wdc_monolingualtext(data, 'P1549')   as p1549_demonym
    ,get_wdc_monolingualtext(data, 'P1448')   as p1448_official_name
    ,get_wdc_monolingualtext(data, 'P1705')   as p1705_native_label
    ,get_wdc_monolingualtext(data, 'P1449')   as p1449_nick_name

    ,get_wd_name_array(data)           as wd_name_array 
    ,get_wd_altname_array(data)        as wd_altname_array

    ,CDB_TransformToWebmercator( ST_SetSRID(ST_MakePoint(
             cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
            )
    , 4326)) as wd_point_merc

    from wdplace.wd_country
)
select *
from c
where fips10_4 is not null
    or country_iso2 is not null
order by wd_id
;


CREATE INDEX  ON  wdplace.wd_match_country USING GIST(wd_point_merc);
CREATE INDEX  ON  wdplace.wd_match_country (una_wd_name_en_clean);
CREATE INDEX  ON  wdplace.wd_match_country (    wd_name_en_clean);
CREATE INDEX  ON  wdplace.wd_match_country (wd_id);
ANALYSE   wdplace.wd_match_country;




-- drop view if exists wof_for_matching;
drop table if exists wof_match_country CASCADE;
create table         wof_match_country  as
select
     wof.id
    ,wof.properties->>'wof:name'            as wof_name
    ,unaccent(wof.properties->>'wof:name')  as una_wof_name
    ,wof.properties->>'wof:country'         as wof_country
    ,wof.wd_id                              as wof_wd_id
    ,get_wof_name_array(wof.properties)     as wof_name_array
    ,CDB_TransformToWebmercator(COALESCE( wof.geom::geometry, wof.centroid::geometry ))  as wof_geom_merc
from wof_country as wof
where  wof.is_superseded=0
   and wof.is_deprecated=0
order by wof_country
;

CREATE INDEX ON  wof_match_country  USING GIST(wof_geom_merc);
CREATE INDEX ON  wof_match_country  (una_wof_name);
CREATE INDEX ON  wof_match_country  (wof_name);
ANALYSE  wof_match_country ;



\set searchdistance      .
\set safedistance   400000
\set wd_input_table           wdplace.wd_match_country
\set wof_input_table          wof_match_country

\set wd_wof_match             wd_mcountry_wof_match
\set wd_wof_match_agg         wd_mcountry_wof_match_agg
\set wd_wof_match_agg_sum     wd_mcountry_wof_match_agg_summary
\set wd_wof_match_notfound    wd_mcountry_wof_match_notfound

\set mcond1  ( wof.wof_country = wd.country_iso2 )
\set mcond2  
\set mcond3
\ir 'template_matching.sql'





drop table if exists  codes.wd2country_new CASCADE;
create table          codes.wd2country_new  as
    select
         coalesce( _suggested_wd_id, wof_wd_id ) as wd_id
        ,wof_country
        ,wof_name
    from :wd_wof_match_agg
    order by coalesce( _suggested_wd_id, wof_wd_id )
;

CREATE UNIQUE INDEX ON codes.wd2country_new (wd_id);
CREATE UNIQUE INDEX ON codes.wd2country_new (wof_country);
ANALYSE codes.wd2country_new;


