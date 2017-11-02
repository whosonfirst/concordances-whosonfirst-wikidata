



drop table if exists wof_disambiguation;
create table wof_disambiguation as
select 
     wof.id
    ,wof.properties->>'wof:name'                    as wof_name 
    ,wof.properties->>'wof:country'                 as wof_country
    ,wof.properties->'wof:concordances'->>'wd:id'   as wd_id
from public.whosonfirst          as wof
    ,wikidata.wd_disambiguation  as wdd
where 
    wof.properties->'wof:concordances'->>'wd:id' =  wdd.wikidataid 
;


create or replace view wof_disambiguation_report
AS
select 
    wof_country
    ,wof_name
    ,id 
    ,wd_id
    ,'https://whosonfirst.mapzen.com/spelunker/id/'||id    as wof_spelunker_url
    ,'https://www.wikidata.org/wiki/'||wd_id               as wd_url
from  
    wof_disambiguation 
order by 
    wof_country
    ,wof_name
;


\copy (select * from wof_disambiguation_report) TO '\wof\wof_disambiguation_report.csv' CSV;
