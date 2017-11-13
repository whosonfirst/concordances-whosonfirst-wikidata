



drop table if exists wof_disambiguation CASCADE;
create table wof_disambiguation as
SELECT
     wof.metatable
    ,wof.id
    ,wof.properties->>'wof:name'                    as wof_name 
    ,wof.properties->>'wof:country'                 as wof_country
    ,wd_id
    ,wof.properties->>'wof:population'              as wof_population
    ,wof.properties->>'wof:population_rank'         as wof_population_rank 
from public.wof                  as wof
    ,wikidata.wd_disambiguation  as wdd
where 
    wof.wd_id =  wdd.wikidataid 
;

ANALYSE wof_disambiguation;


create or replace view wof_disambiguation_report
AS
select 
     metatable
    ,wof_country
    ,wof_name
    ,id 
    ,wd_id
    ,wof_population
    ,wof_population_rank
    ,'https://whosonfirst.mapzen.com/spelunker/id/'||id    as wof_spelunker_url
    ,'https://www.wikidata.org/wiki/'||wd_id               as wd_url
from  
    wof_disambiguation 
order by 
     metatable
    ,wof_country
    ,wof_name
;

create or replace view wof_disambiguation_sum_report as
select
      metatable
    , wof_country
    , count(*) as number_of_disambiguation
from  wof_disambiguation_report
group by metatable , wof_country
order by metatable , wof_country
; 


SELECT metatable, count(*) AS N 
FROM wof_disambiguation_report
GROUP BY  metatable
ORDER BY N DESC
--LIMIT 10
;

\cd :reportdir
\copy (select * from wof_disambiguation_report) TO 'wof_disambiguation_report.csv' CSV;
