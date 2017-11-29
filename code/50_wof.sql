

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
