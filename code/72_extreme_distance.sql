--


drop table if exists  wfwd.wof_extreme_distance CASCADE;
create table          wfwd.wof_extreme_distance as
select
     wof.metatable
    ,wof.id
    ,wof.properties->>'wof:name'                    as wof_name 
    ,wof.properties->>'wof:country'                 as wof_country
    ,wof.wd_id
    ,wof.is_superseded
    ,wof.is_deprecated    
    ,wof.properties->>'wof:population'              as wof_population
    ,wof.properties->>'wof:population_rank'         as wof_population_rank     
    ,ST_Distance(
          CDB_TransformToWebmercator(wdp.wd_point)   --   ST_Transform(wdp.wd_point, 3857 )
        , CDB_TransformToWebmercator(wof.centroid::geometry)    --   ST_Transform(wof.centroid::geometry, 3857 )  
        )     as distance_centroid
    ,ST_Distance(
         CDB_TransformToWebmercator(wdp.wd_point)   --  ST_Transform(wdp.wd_point, 3857 )
        ,CDB_TransformToWebmercator(wof.geom::geometry)  -- ST_Transform(wof.geom::geometry, 3857 )  
        )     as distance_geom        
from  wf.wof                 as wof
      ,wd.wd_rank_point      as wdp 
where  wof.wd_id =  wdp.wd_id
;

ANALYSE wfwd.wof_extreme_distance;



create or replace view wfwd.wof_extreme_distance_report
AS
with wof_dview AS (
  select  
    *
    ,coalesce( distance_geom,distance_centroid) as distance
  from wfwd.wof_extreme_distance 
)
select
     ( distance /1000)::integer as distance_km
    , metatable
    , id
    , wof_name
    , wof_country
    , wd_id
    , is_superseded
    , is_deprecated    
    , wof_population
    , wof_population_rank    
    ,'https://whosonfirst.mapzen.com/spelunker/id/'||id    as wof_spelunker_url
    ,'https://www.wikidata.org/wiki/'||wd_id               as wd_url
from  wof_dview 
where distance > 50000 -- > 50km
order by distance desc  
; 


create or replace view wfwd.wof_extreme_distance_sum_report as
select
      metatable
    , wof_country
    , count(*) as number_of_distance_problems
from  wfwd.wof_extreme_distance_report
group by metatable , wof_country
order by metatable , wof_country
; 


\cd :reportdir
\copy (select * from wfwd.wof_extreme_distance_report) TO 'wof_extreme_distance_report.csv' CSV;

