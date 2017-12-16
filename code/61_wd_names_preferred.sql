

drop table if exists wd.wd_names_preferred;
CREATE UNLOGGED TABLE wd.wd_names_preferred as
with wd_names_iso2 as
 (
 select
      wd_id
    , data->'labels'->jsonb_object_keys(data->'labels')->>'language'::text   as wd_lang
    , data->'labels'->jsonb_object_keys(data->'labels')->>'value'            as wof_value
    FROM wd.wdx
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

create index on wd.wd_names_preferred ( wd_id ) WITH (fillfactor = 100);
--ANALYZE         wd.wd_names_preferred;

