#!/bin/bash

set -e
set -u

./code/cmd_load_wof.sh     wof_locality        wof-locality-latest.csv         
./code/cmd_load_wof.sh     wof_localadmin      wof-localadmin-latest.csv       
./code/cmd_load_wof.sh     wof_neighbourhood   wof-neighbourhood-latest.csv    
./code/cmd_load_wof.sh     wof_county          wof-county-latest.csv           
./code/cmd_load_wof.sh     wof_campus          wof-campus-latest.csv           
./code/cmd_load_wof.sh     wof_region          wof-region-latest.csv           
./code/cmd_load_wof.sh     wof_microhood       wof-microhood-latest.csv        
./code/cmd_load_wof.sh     wof_macrohood       wof-macrohood-latest.csv        
./code/cmd_load_wof.sh     wof_macrocounty     wof-macrocounty-latest.csv      
./code/cmd_load_wof.sh     wof_timezone        wof-timezone-latest.csv         
./code/cmd_load_wof.sh     wof_marinearea      wof-marinearea-latest.csv       
./code/cmd_load_wof.sh     wof_country         wof-country-latest.csv          
./code/cmd_load_wof.sh     wof_empire          wof-empire-latest.csv           
./code/cmd_load_wof.sh     wof_borough         wof-borough-latest.csv          
./code/cmd_load_wof.sh     wof_macroregion     wof-macroregion-latest.csv      
./code/cmd_load_wof.sh     wof_ocean           wof-ocean-latest.csv            
./code/cmd_load_wof.sh     wof_dependency      wof-dependency-latest.csv       
./code/cmd_load_wof.sh     wof_planet          wof-planet-latest.csv           
./code/cmd_load_wof.sh     wof_continent       wof-continent-latest.csv        
./code/cmd_load_wof.sh     wof_disputed        wof-disputed-latest.csv         

