
drop table if exists wof_rec;
create table wof_rec as
select 
    id
   ,properties->>'wof:name'                    as wof_name 
   ,wd_id
   ,jsonb_object_keys(properties)              as wof_property
   ,properties->jsonb_object_keys(properties)  as wof_jvalue
   ,jsonb_typeof( properties->jsonb_object_keys(properties) ) as wof_jtype 
from wof;

create index on wof_rec  ( id );
create index on wof_rec  ( wd_id );
create index on wof_rec  ( wof_property );

analyze wof_rec;
