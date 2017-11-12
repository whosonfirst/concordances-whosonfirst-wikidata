


select 
    id
   ,properties->>'wof:name'                    as wof_name 
   ,properties->'wof:concordances'->>'wd:id'   as wd_id
from wof
where
  properties->'wof:concordances'->>'wd:id'  IS NOT NULL
and
  properties->'wof:concordances'->>'wd:id'   NOT IN
 (select  data->>'id'::text as wd_id  FROM wikidata.wd )
;