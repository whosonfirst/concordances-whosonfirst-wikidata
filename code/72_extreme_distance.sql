--


drop table if exists wof_extreme_distance CASCADE;
create table wof_extreme_distance as
select
     wof.metatable
    ,wof.id
    ,wof.properties->>'wof:name'                    as wof_name 
    ,wof.properties->>'wof:country'                 as wof_country
    ,wof.properties->'wof:concordances'->>'wd:id'   as wd_id
   -- ,ST_Transform(wdp.wd_point, 3857 )              as g1
   -- ,ST_Transform(wof.centroid::geometry, 3857 )    as g2
    ,ST_Distance(
         ST_Transform(wdp.wd_point, 3857 )
        ,ST_Transform(wof.centroid::geometry, 3857 )  
        )     as distance_centroid
    ,ST_Distance(
         ST_Transform(wdp.wd_point, 3857 )
        ,ST_Transform(wof.geom::geometry, 3857 )  
        )     as distance_geom        
from public.wof                  as wof
    ,wikidata.wd_rank_point      as wdp
where 
    wof.properties->'wof:concordances'->>'wd:id' =  wdp.wd_id
--LIMIT 1000
;



create or replace view wof_extreme_distance_report
AS
with wof_dview AS (
  select  
    *
    ,coalesce( distance_geom,distance_centroid) as distance
  from wof_extreme_distance 
)
select
     ( distance /1000)::integer as distance_km
    , metatable
    , id
    , wof_name
    , wof_country
    , wd_id
    ,'https://whosonfirst.mapzen.com/spelunker/id/'||id    as wof_spelunker_url
    ,'https://www.wikidata.org/wiki/'||wd_id               as wd_url
from  wof_dview 
where distance > 50000 -- > 50km
order by distance desc  
; 


\cd :reportdir
\copy (select * from wof_extreme_distance_report) TO 'wof_extreme_distance_report.csv' CSV;
