#!/bin/bash
set -o errexit
set -o pipefail
set -o nounset

/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_populated_places
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_0_countries
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_0_disputed_areas
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_0_map_subunits
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_ports
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_time_zones
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_airports

/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_parks_and_protected_lands_area
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_parks_and_protected_lands_line
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_parks_and_protected_lands_point
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_parks_and_protected_lands_scale_rank

/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_seams
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces_lakes
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces_lines
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces_scale_rank
/wof/code/cmd_load_natural_earth.sh 10m_cultural ne_10m_admin_1_states_provinces_scale_rank_minor_islands

/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_antarctic_ice_shelves_lines         
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_antarctic_ice_shelves_polys         
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_coastline                           
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_geographic_lines                    
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_geography_marine_polys              
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_geography_regions_elevation_points  
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_geography_regions_points            
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_geography_regions_polys             
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_glaciated_areas          
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_lakes                    
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_lakes_europe             
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_lakes_historic           
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_lakes_north_america      
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_lakes_pluvial            
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_land                     
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_land_ocean_label_points  
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_land_ocean_seams            
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_land_scale_rank             
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_minor_islands               
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_minor_islands_coastline     
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_minor_islands_label_points  
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_ocean
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_ocean_scale_rank
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_playas
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_reefs
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_rivers_europe
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_rivers_lake_centerlines
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_rivers_lake_centerlines_scale_rank
/wof/code/cmd_load_natural_earth.sh  10m_physical  ne_10m_rivers_north_america

echo "END:======== natural earth loading ==========="
