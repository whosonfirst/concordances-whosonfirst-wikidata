

create or replace view wof AS
              select 'wof_borough'          as metatable, * from wf.wof_borough         
        union select 'wof_campus'           as metatable, * from wf.wof_campus          
        union select 'wof_continent'        as metatable, * from wf.wof_continent       
        union select 'wof_country'          as metatable, * from wf.wof_country         
        union select 'wof_county'           as metatable, * from wf.wof_county          
        union select 'wof_dependency'       as metatable, * from wf.wof_dependency      
        union select 'wof_disputed'         as metatable, * from wf.wof_disputed        
        union select 'wof_empire'           as metatable, * from wf.wof_empire          
        union select 'wof_localadmin'       as metatable, * from wf.wof_localadmin      
        union select 'wof_locality'         as metatable, * from wf.wof_locality        
        union select 'wof_macrocounty'      as metatable, * from wf.wof_macrocounty     
        union select 'wof_macrohood'        as metatable, * from wf.wof_macrohood       
        union select 'wof_macroregion'      as metatable, * from wf.wof_macroregion     
        union select 'wof_marinearea'       as metatable, * from wf.wof_marinearea      
        union select 'wof_microhood'        as metatable, * from wf.wof_microhood       
        union select 'wof_neighbourhood'    as metatable, * from wf.wof_neighbourhood   
        union select 'wof_ocean'            as metatable, * from wf.wof_ocean           
        union select 'wof_planet'           as metatable, * from wf.wof_planet          
        union select 'wof_region'           as metatable, * from wf.wof_region          
        union select 'wof_timezone'         as metatable, * from wf.wof_timezone        
;  
