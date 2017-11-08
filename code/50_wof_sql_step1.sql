


create or replace view wof AS
              select 'wof_borough'          as metatable, * from wof_borough         
        union select 'wof_campus'           as metatable, * from wof_campus          
        union select 'wof_continent'        as metatable, * from wof_continent       
        union select 'wof_country'          as metatable, * from wof_country         
        union select 'wof_county'           as metatable, * from wof_county          
        union select 'wof_dependency'       as metatable, * from wof_dependency      
        union select 'wof_disputed'         as metatable, * from wof_disputed        
        union select 'wof_empire'           as metatable, * from wof_empire          
        union select 'wof_localadmin'       as metatable, * from wof_localadmin      
        union select 'wof_locality'         as metatable, * from wof_locality        
        union select 'wof_macrocounty'      as metatable, * from wof_macrocounty     
        union select 'wof_macrohood'        as metatable, * from wof_macrohood       
        union select 'wof_macroregion'      as metatable, * from wof_macroregion     
        union select 'wof_marinearea'       as metatable, * from wof_marinearea      
        union select 'wof_microhood'        as metatable, * from wof_microhood       
        union select 'wof_neighbourhood'    as metatable, * from wof_neighbourhood   
        union select 'wof_ocean'            as metatable, * from wof_ocean           
        union select 'wof_planet'           as metatable, * from wof_planet          
        union select 'wof_region'           as metatable, * from wof_region          
        union select 'wof_timezone'         as metatable, * from wof_timezone        
;  


drop table if exists wof_rec;
create table wof_rec as
select 
    id
   ,properties->>'wof:name'                    as wof_name 
   ,properties->'wof:concordances'->>'wd:id'   as wd_id
   ,jsonb_object_keys(properties)              as wof_property
   ,properties->jsonb_object_keys(properties)  as wof_jvalue
   ,jsonb_typeof( properties->jsonb_object_keys(properties) ) as wof_jtype 
from wof;

create index on wof_rec  ( id );
create index on wof_rec  ( wd_id );
create index on wof_rec  ( wof_property );

analyze wof_rec;






drop table if exists wof_name;
create table wof_name as
select  
      id
    , wof_name
    , wd_id
    , wof_property
    , wof_value 
    , wof_arrayorder
    , jsonb_array_length(wof_rec.wof_jvalue) as wof_arrayorder_max
from wof_rec 
    ,jsonb_array_elements_text(wof_jvalue) with ordinality as a(wof_value,wof_arrayorder)
where wof_jtype='array' and substr(wof_property,1,5)='name:' 
order by id, wof_property, wof_arrayorder
;

create index on wof_name  ( id );
create index on wof_name  ( wd_id );
create index on wof_name  ( wof_property );

analyze wof_name;

