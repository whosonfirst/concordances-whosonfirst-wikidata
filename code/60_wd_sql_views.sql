
-- important : it can be duplicated, beacause  "claim.P31" can be more "Q4167410" value ( 1:N )
create or replace view wikidata.wd_disambiguation
as
with p31 as (
    select
     data->>'id'::text                                                                          as wikidataid
   , (jsonb_array_elements( data->'claims'->'P31' )->'mainsnak'->'datavalue'->'value'->>'id')   as wof_P31_value
    FROM wikidata.wd
)
select distinct wikidataid from P31
--TODO: find (sub-)*subclass of a disambiguation page.
where wof_P31_value = 'Q4167410'  -- disambiguation page
order by  wikidataid
;



create or replace view wikidata.wd_sitelinks as
 select
      data->>'id'::text                                                     as wd_id
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->>'site'      as wd_site
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->>'title'     as wd_title
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->'badges'     as wd_badges
    , jsonb_array_length(data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->'badges')   as wd_badges_number
    FROM wikidata.wd
;


create or replace view wikidata.wd_descriptions as
 select
      data->>'id'::text                                                             as wd_id
    , data->'descriptions'->jsonb_object_keys(data->'descriptions')->>'language'    as wd_language
    , data->'descriptions'->jsonb_object_keys(data->'descriptions')->>'value'       as wd_description
    FROM wikidata.wd
;


--  it can be multiple language values , so wd_id + wd_language is not unique !! ;
create or replace view wikidata.wd_aliases as
with aliases as ( 
    select
      data->>'id'::text                                                           as wd_id
    , jsonb_array_elements( data->'aliases'->jsonb_object_keys(data->'aliases'))  as wd_aliases_object
    FROM wikidata.wd
)
select 
   wd_id
  ,wd_aliases_object->>'language' as wd_language
  ,wd_aliases_object->>'value'    as wd_alias

from aliases;


create or replace view wikidata.wd_labels as
 select
      data->>'id'::text as wd_id
    , data->'labels'->jsonb_object_keys(data->'labels')->>'language'   as wd_language
    , data->'labels'->jsonb_object_keys(data->'labels')->>'value'      as wd_label
    FROM wikidata.wd
;

