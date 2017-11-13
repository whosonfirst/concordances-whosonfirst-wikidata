


select 
    id
   ,properties->>'wof:name'               as wof_name 
   ,wd_id
from wof
where
   wd_id != ''
and
  wd_id  NOT IN (select  data->>'id'::text as wd_id  FROM wikidata.wd )
;