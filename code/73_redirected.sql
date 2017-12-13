

drop table if exists    wfwd.wof_wd_redirects CASCADE;
create table            wfwd.wof_wd_redirects as
select 
     wof.id
    ,wof.metatable
    ,wof.properties->>'wof:name'                    as wof_name 
    ,wof.properties->>'wof:country'                 as wof_country
    ,wof.wd_id
    ,wof.is_superseded
    ,wof.is_deprecated    
    ,wof.properties->>'wof:population'              as wof_population
    ,wof.properties->>'wof:population_rank'         as wof_population_rank     
    ,wd_redirects.wd_to
from wf.wof
    ,wd.wd_redirects as wd_redirects  
where wof.wd_id=wd_redirects.wd_from
order by wof.id   
;


ANALYSE wfwd.wof_wd_redirects;

create or replace view wfwd.wof_wd_redirects_report
AS
select
    *
    ,'https://whosonfirst.mapzen.com/spelunker/id/'||id    as wof_spelunker_url
    ,'https://www.wikidata.org/wiki/'||wd_id               as wd_url
    ,'https://www.wikidata.org/wiki/'||wd_to               as wd_to_url    
from  wfwd.wof_wd_redirects
order by id
; 


create or replace view wfwd.wof_wd_redirects_sum_report as
select
      metatable
    , wof_country
    , count(*) as number_of_wdredirects_problems
from  wfwd.wof_wd_redirects_report
group by metatable , wof_country
order by metatable , wof_country
; 



\cd :reportdir
\copy (select * from wfwd.wof_wd_redirects_report) TO 'wof_wd_redirects_report.csv' CSV;
