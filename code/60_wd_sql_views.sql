
-- important : it can be duplicated, beacause  "claim.P31" can be more "Q4167410" value ( 1:N )
DROP VIEW IF EXISTS  wd.wd_disambiguation CASCADE;
create or replace view wd.wd_disambiguation
as
with p31 as (
    select
     wd_id                                                                                      as wikidataid
   , (jsonb_array_elements( data->'claims'->'P31' )->'mainsnak'->'datavalue'->'value'->>'id')   as wof_P31_value
    FROM wd.wdx
)
select distinct wikidataid
     , ARRAY[]::text[] as a_wof_type from P31     -- TODO: this is only a temporary fix
--TODO: find (sub-)*subclass of a disambiguation page.
where wof_P31_value = 'Q4167410'  -- disambiguation page
order by  wikidataid
;



create or replace view wd.wd_sitelinks as
 select
      wd_id
    , a_wof_type  
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->>'site'      as wd_site
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->>'title'     as wd_title
    , data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->'badges'     as wd_badges
    , jsonb_array_length(data->'sitelinks'->jsonb_object_keys(data->'sitelinks')->'badges')   as wd_badges_number
    FROM wd.wdx
;


create or replace view wd.wd_descriptions as
 select
      wd_id
    , a_wof_type  
    , data->'descriptions'->jsonb_object_keys(data->'descriptions')->>'language'    as wd_language
    , data->'descriptions'->jsonb_object_keys(data->'descriptions')->>'value'       as wd_description
    FROM wd.wdx
;


--  it can be multiple language values , so wd_id + wd_language is not unique !! ;
create or replace view wd.wd_aliases as
with aliases as ( 
    select
      wd_id
    , a_wof_type  
    , jsonb_array_elements( data->'aliases'->jsonb_object_keys(data->'aliases'))  as wd_aliases_object
    FROM wd.wdx
)
select 
   wd_id
  ,a_wof_type 
  ,wd_aliases_object->>'language' as wd_language
  ,wd_aliases_object->>'value'    as wd_alias

from aliases;


create or replace view wd.wd_labels as
 select
      wd_id
    , a_wof_type  
    , data->'labels'->jsonb_object_keys(data->'labels')->>'language'   as wd_language
    , data->'labels'->jsonb_object_keys(data->'labels')->>'value'      as wd_label
    FROM wd.wdx
;

