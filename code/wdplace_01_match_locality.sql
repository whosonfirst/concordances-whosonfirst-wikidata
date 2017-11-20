

create extension if not exists unaccent;


--  TODO: get the latest  P31 status.
--  Q1018914  | Q1018914   has an english name ( has an english label)
--  wikidata has a coordinate
--  TODO: wikidata has any of enwiki, eswiki, dewiki, ptwiki, ruwiki, znwiki page  [  so not a cebuano import ] 
--  distance < 50km
--  only 1 match!


CREATE OR REPLACE FUNCTION public.is_cebuano(data jsonb)
RETURNS bool
IMMUTABLE
LANGUAGE sql
AS $$
    with cebu_calc as      
    (
        SELECT sum(    
        case when site in ( 'enwiki','dewiki','ptwiki','eswiki','ruwiki','frwiki','nlwiki')   then 10
                when site in ( 'svwiki','shwiki' )   then  3
                when site in ( 'cebwiki')            then -9      
                                                     else  5
        end
        ) site_points
        FROM jsonb_object_keys(data->'sitelinks') as site
    )
    select case when site_points > 0 then false
                                     else true
        end 
    from cebu_calc    
    ;
$$;


-- drop view if exists  wdplace.wd_for_matching CASCADE;
drop table if exists  wdplace.wd_for_matching CASCADE;
create table  wdplace.wd_for_matching  as
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
    ;

CREATE INDEX CONCURRENTLY wdplace_wd_for_matching_x_point           ON  wdplace.wd_for_matching USING GIST(wd_point);
CREATE INDEX CONCURRENTLY wdplace_wd_for_matching_una_name_en_clean ON  wdplace.wd_for_matching (una_wd_name_en_clean);
CREATE INDEX CONCURRENTLY wdplace_wd_for_matching_name_en_clean     ON  wdplace.wd_for_matching (    wd_name_en_clean);
ANALYSE   wdplace.wd_for_matching ;


-- drop view if exists wof_for_matching;
drop table if exists wof_for_matching CASCADE;
create table         wof_for_matching  as
select
     wof.id
    ,wof.properties->>'wof:name'            as wof_name 
    ,unaccent(wof.properties->>'wof:name')  as una_wof_name 
    ,wof.properties->>'wof:country' as wof_country
    ,wof.wd_id                      as wof_wd_id
    ,COALESCE( wof.geom::geometry, wof.centroid::geometry )  as wof_geom
from wof_locality as wof
where  wof.is_superseded=0 
   and wof.is_deprecated=0
;

CREATE INDEX CONCURRENTLY wof_for_matching_x_point        ON  wof_for_matching   USING GIST(wof_geom);
CREATE INDEX CONCURRENTLY wof_for_matching_una_wof_name   ON  wof_for_matching   (una_wof_name);
CREATE INDEX CONCURRENTLY wof_for_matching_wof_name       ON  wof_for_matching   (wof_name);
ANALYSE   wof_for_matching ;




drop table if exists  wd_wof_match CASCADE;
create table  wd_wof_match  as
    select
         wof.* 
        ,ST_Distance(
              CDB_TransformToWebmercator(wd.wd_point)   
            , CDB_TransformToWebmercator(wof.wof_geom) 
            )::bigint     as _distance
        , wd.*        
        , case when  wof.wof_name     = wd.wd_name_en_clean 
              then 'full-name-match'
              else 'unaccent-name-match'
          end as  _name_match_type    
    from wdplace.wd_for_matching as wd 
        ,wof_for_matching        as wof
    where (  
          --   ( wof.wof_name     = wd.wd_name_en_clean )
          --   or
              ( wof.una_wof_name = wd.una_wd_name_en_clean )
          )  
        and ST_Distance(
              CDB_TransformToWebmercator(wd.wd_point)   
            , CDB_TransformToWebmercator(wof.wof_geom) 
            )::bigint  <= 50000
    order by wof.id, _distance
;
ANALYSE     wd_wof_match ;



drop table if exists  wd_wof_match_agg CASCADE;
create table  wd_wof_match_agg  as
with wd_agg as 
(
    select id, wof_name, wof_country,wof_wd_id
        ,  array_agg(wd_id     order by _distance) as a_wd_id
        ,  array_agg(_distance order by _distance) as a_wd_id_distance 
        ,  array_agg(_name_match_type  order by _name_match_type ) as a_wd_name_match_type                 
    from wd_wof_match
    group by id, wof_name, wof_country,wof_wd_id
    order by id, wof_name, wof_country,wof_wd_id
)
select *
      ,case 
         when  array_length(a_wd_id,1) =1  then   a_wd_id[1]
           else NULL
        end as _suggested_wd_id
      ,array_length(a_wd_id,1) as wd_number_of_matches
      ,case 
          when a_wd_id_distance[1] <=  5000 then '00-05km' 
          when a_wd_id_distance[1] <= 10000 then '05-10km' 
          when a_wd_id_distance[1] <= 15000 then '10-15km'
          when a_wd_id_distance[1] <= 20000 then '15-20km'
          when a_wd_id_distance[1] <= 25000 then '20-25km'
          when a_wd_id_distance[1] <= 30000 then '25-30km'
          when a_wd_id_distance[1] <= 35000 then '30-35km'
          when a_wd_id_distance[1] <= 40000 then '35-40km'
          when a_wd_id_distance[1] <= 45000 then '40-45km'
          when a_wd_id_distance[1] <= 50000 then '45-50km'          
            else     '-checkme-'     
      end as _firstmatch_distance_category    
     ,case 
         when  array_length(a_wd_id,1) =1   and  wof_wd_id  = a_wd_id[1]                    then 'validated-'||a_wd_name_match_type[1]    
         when  array_length(a_wd_id,1) =1   and  wof_wd_id != a_wd_id[1] and wof_wd_id !='' then 'suggested for replace-'||a_wd_name_match_type[1] 
         when  array_length(a_wd_id,1) =1   and  wof_wd_id != a_wd_id[1] and wof_wd_id  ='' then 'suggested for add-'||a_wd_name_match_type[1] 
         else 'multiple_match (please not import this!)'
      end as _matching_category 
from wd_agg
-- order by wd_number_of_matches  desc
;
ANALYSE wd_wof_match_agg ;


drop table if exists  wd_wof_match_agg_summary CASCADE;
create table  wd_wof_match_agg_summary  as
    select _matching_category,  wd_number_of_matches, _firstmatch_distance_category, count(*) as N  
    from wd_wof_match_agg
    -- where  wof_country='HU'
    group by  _matching_category, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category, wd_number_of_matches, _firstmatch_distance_category
;
ANALYSE wd_wof_match_agg_summary ;


-- select id,wof_name,wd_id,wof_wd_id from wd_wof_match where wof_country='HU' and wof_wd_id != wd_id   limit 1000;

-- select * from wd_wof_match_agg where wof_country='HU'; 

/*
wd_id
wd_name_en
wd_name_en_clean
p1566_geonames
wd_point
p31_instance_of
p17_country_id
p36_capital
p1376_capital_of
p190_sister_city
p460_same_as
p1082_population
p300_iso3166_2
p901_fips10_4
*/
