
drop table if exists wf.wof_rec;
create  UNLOGGED table wf.wof_rec as
select 
    id
   ,properties->>'wof:name'                    as wof_name 
   ,wd_id
   ,jsonb_object_keys(properties)              as wof_property
   ,properties->jsonb_object_keys(properties)  as wof_jvalue
   ,jsonb_typeof( properties->jsonb_object_keys(properties) ) as wof_jtype 
from wf.wof;

create index on wf.wof_rec  ( id )              WITH (fillfactor = 100);
create index on wf.wof_rec  ( wd_id )           WITH (fillfactor = 100);
create index on wf.wof_rec  ( wof_property )    WITH (fillfactor = 100);

analyze wf.wof_rec;



drop table if exists wf.wof_pname;
create  UNLOGGED table wf.wof_pname as
select  
      id
    , wof_name
    , wd_id
    , wof_property
    , wof_value 
    , wof_arrayorder
    , jsonb_array_length(wof_rec.wof_jvalue) as wof_arrayorder_max
from wf.wof_rec 
    ,jsonb_array_elements_text(wof_jvalue) with ordinality as a(wof_value,wof_arrayorder)
where wof_jtype='array' and substr(wof_property,1,5)='name:' 
order by id, wof_property, wof_arrayorder
;

create index on wf.wof_pname  ( id )            WITH (fillfactor = 100);
create index on wf.wof_pname  ( wd_id )         WITH (fillfactor = 100);
create index on wf.wof_pname  ( wof_property )  WITH (fillfactor = 100);

analyze wf.wof_pname;