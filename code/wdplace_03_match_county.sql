



drop table if exists  wdplace.wd_match_county CASCADE;
create table          wdplace.wd_match_county  as
    select
     data->>'id'::text                  as wd_id  
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en
    ,get_wdlabeltext(data->>'id'::text) as wd_name_en_clean
    ,unaccent(get_wdlabeltext(data->>'id'::text))   as una_wd_name_en_clean

    ,get_countrycode( (get_wdc_item(data,'P17'))->>0 )   as wd_country 

    ,get_wdc_item_label(data,'P31')    as p31_instance_of
    ,get_wdc_item_label(data,'P17')    as p17_country_id    

    ,(get_wdc_value(data, 'P901'))->>0  as fips10_4

    ,get_wdc_value(data, 'P300')    as p300_iso3166_2
    ,get_wdc_value(data, 'P901')    as p901_fips10_4
    ,get_wdc_value(data, 'P1566')   as p1566_geonames
        
    ,get_wdc_monolingualtext(data, 'P1813')   as p1813_short_name
    ,get_wdc_monolingualtext(data, 'P1549')   as p1549_demonym
    ,get_wdc_monolingualtext(data, 'P1448')   as p1448_official_name
    ,get_wdc_monolingualtext(data, 'P1705')   as p1705_native_label
    ,get_wdc_monolingualtext(data, 'P1449')   as p1449_nick_name    

    ,ST_SetSRID(ST_MakePoint( 
             cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
            ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
            )
    , 4326) as wd_point
    
    from wdplace.wd_county    
;

CREATE INDEX  wdplace_wd_match_county_x_point           ON  wdplace.wd_match_county USING GIST(wd_point);
CREATE INDEX  wdplace_wd_match_county_una_name_en_clean ON  wdplace.wd_match_county (una_wd_name_en_clean);
CREATE INDEX  wdplace_wd_match_county_name_en_clean     ON  wdplace.wd_match_county (    wd_name_en_clean);
CREATE INDEX  wdplace_wd_match_county_wd_id             ON  wdplace.wd_match_county (wd_id);
ANALYSE   wdplace.wd_match_county;




drop table if exists wof_match_county CASCADE;
create table         wof_match_county  as
select
     wof.id
    ,wof.properties->>'wof:name'            as wof_name 
    ,unaccent(wof.properties->>'wof:name')  as una_wof_name 
    ,wof.properties->>'wof:country'         as wof_country
    ,wof.wd_id                              as wof_wd_id
    ,COALESCE( wof.geom::geometry, wof.centroid::geometry )  as wof_geom
from wof_county as wof
where  wof.is_superseded=0 
   and wof.is_deprecated=0
;



CREATE INDEX  wof_match_county_x_point        ON  wof_match_county  USING GIST(wof_geom);
CREATE INDEX  wof_match_county_una_wof_name   ON  wof_match_county  (una_wof_name);
CREATE INDEX  wof_match_county_wof_name       ON  wof_match_county  (wof_name);
ANALYSE  wof_match_county ;



drop table if exists  wd_mcounty_wof_match CASCADE;
create table          wd_mcounty_wof_match  as
    select
         wof.id 
        ,wof.wof_name
        ,wof.wof_country
        ,wof.wof_wd_id
        ,ST_Distance(
              CDB_TransformToWebmercator(wd.wd_point)   
            , CDB_TransformToWebmercator(wof.wof_geom) 
            )::bigint     as _distance
        ,case when  wof.wof_name     = wd.wd_name_en_clean 
              then 'full-name-match'
              else 'unaccent-name-match'
          end as  _name_match_type                
        ,wd.*        
    from wdplace.wd_match_county  as wd 
        ,wof_match_county         as wof
    where      wof.wof_country  = wd.wd_country 
        and wof.una_wof_name = wd.una_wd_name_en_clean
        and ST_Distance(
              CDB_TransformToWebmercator(wd.wd_point)   
            , CDB_TransformToWebmercator(wof.wof_geom) 
            )::bigint  <= 120000

    --order by wof.id
    --limit 1000;
;
ANALYSE     wd_mcounty_wof_match ;






drop table if exists  wd_mcounty_wof_match_agg CASCADE;
create table          wd_mcounty_wof_match_agg  as
with wd_agg as 
(
    select id, wof_name, wof_country,wof_wd_id
        ,  array_agg(wd_id     order by     wd_id) as a_wd_id       
    from wd_mcounty_wof_match
    group by id, wof_name, wof_country,wof_wd_id 
    order by id, wof_name, wof_country,wof_wd_id  
)
, wd_agg_extended as
(
 select wd_agg.*
      ,ARRAY[wof_wd_id] &&  a_wd_id as _wd_ok

      ,case 
         when  not (ARRAY[wof_wd_id] &&  a_wd_id)  and  array_length(a_wd_id,1) =1  then   a_wd_id[1]
           else NULL
        end as _suggested_wd_id
      ,array_length(a_wd_id,1) as wd_number_of_matches
     ,case 
         when  array_length(a_wd_id,1)  =1   and  wof_wd_id  = a_wd_id[1]                    then 'validated' 
         when  array_length(a_wd_id,1) !=1   and (ARRAY[wof_wd_id] &&  a_wd_id)              then 'validated-multiple match'              
         when  array_length(a_wd_id,1) =1   and  wof_wd_id != a_wd_id[1] and wof_wd_id !=''  then 'suggested for replace-' 
         when  array_length(a_wd_id,1) =1   and  wof_wd_id != a_wd_id[1] and wof_wd_id  =''  then 'suggested for add-' 
         else 'multiple_match (please check! )'
      end as _matching_category
  from wd_agg
)

select wd_agg_extended.* 
      ,get_wdc_item_label(wd.data,'P31') as old_p31_instance_of
      ,wdnew.p31_instance_of             as new_p31_instance_of      
      ,get_wdc_item_label(wd.data,'P17') as old_p17_country_id       
      ,wdnew.p17_country_id              as new_p17_country_id
      ,get_wdlabeltext(wd_agg_extended.wof_wd_id)        as old_wd_label
      ,get_wdlabeltext(wd_agg_extended._suggested_wd_id) as new_wd_label
      ,is_cebuano(wd.data)                               as old_is_cebauno
from wd_agg_extended
left join wikidata.wd              as wd     on wd_agg_extended.wof_wd_id=wd.data->>'id'
left join wdplace.wd_for_matching  as wdnew  on wd_agg_extended._suggested_wd_id=wdnew.wd_id   
;
ANALYSE wd_mcounty_wof_match_agg ;


drop table if exists  wd_mcounty_wof_match_agg_summary CASCADE;
create table          wd_mcounty_wof_match_agg_summary  as
    select _matching_category,  wd_number_of_matches,  count(*) as N  
    from wd_mcounty_wof_match_agg
    group by  _matching_category, wd_number_of_matches
    order by  _matching_category, wd_number_of_matches
    ;
ANALYSE wd_mcounty_wof_match_agg_summary ;

-- Q11965730

-- {Q145,Q230791}
-- {Q184,Q2895}
-- {Q29999,Q55}
-- {Q35,Q756617}