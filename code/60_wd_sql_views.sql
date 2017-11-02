drop table if exists wikidata.wd_names_preferred;
create table wikidata.wd_names_preferred as
with wd_names_iso2 as
 ( 
 select 
       data->>'id'::text as wd_id 
    , data->'labels'->jsonb_object_keys(data->'labels')->>'language'::text   as wd_lang    
    , data->'labels'->jsonb_object_keys(data->'labels')->>'value'            as wof_value 
    FROM wikidata.wd 
 )
select 
       wd.wd_id
      ,wd.wd_lang
      ,langcodes.alpha3_b as wof_lang      
      , case when langcodes.alpha3_b isnull 
             then 'name:' || wd.wd_lang         ||'_x_preferred'
      		 else 'name:' || langcodes.alpha3_b ||'_x_preferred'   
      	end      		
        as wof_property
      ,wd.wof_value    
FROM wd_names_iso2                      as wd
     left join codes.iso_language_codes as langcodes  on wd.wd_lang=langcodes.alpha2
order by wd_id
;








create or replace view wikidata.wd_disambiguation
as 
with p31 as (
    select 
     data->>'id'::text                                                                          as wikidataid 
   , (jsonb_array_elements( data->'claims'->'P31' )->'mainsnak'->'datavalue'->'value'->>'id')   as wof_P31_value   
    FROM wikidata.wd
)
select * from P31
--TODO: find (sub-)*subclass of a disambiguation page.
where wof_P31_value = 'Q4167410'  -- disambiguation page
; 






create or replace view wikidata.wd_point
as 
with 
p625 as (
    select 
         data->>'id'::text                              as wd_id 
        ,jsonb_array_elements( data->'claims'->'P625' ) as p625_object
    FROM wikidata.wd
),
p625_extract as (
	select 
	      wd_id
	     ,p625_object->'mainsnak'->'datavalue'->'value'->>'latitude'  as wd_latitude     
	     ,p625_object->'mainsnak'->'datavalue'->'value'->>'longitude' as wd_longitude
	     ,p625_object->'mainsnak'->'datavalue'->>'type'               as wd_type  
	     ,p625_object->>'rank'                                        as wd_rank  
	from P625
)
select  wd_id
       ,wd_rank
       ,case 
            -- https://www.wikidata.org/wiki/Help:Ranking
	        when wd_rank='preferred'  then 100
	        when wd_rank='normal'     then 1
	        when wd_rank='deprecated' then -100
	        else 0
        end as point_rank
       ,ST_SetSRID(ST_MakePoint( wd_longitude::float8, wd_latitude::float8),4326) as wd_point
from p625_extract    
--limit 1000
; 

