
\ir 'ne_0401_match_georegp_continent.sql'
\ir 'ne_0401_match_georegp_delta.sql'
\ir 'ne_0401_match_georegp_island.sql'
\ir 'ne_0402_match_georegp_depression.sql'
\ir 'ne_0402_match_georegp_desert.sql'
\ir 'ne_0402_match_georegp_geoarea.sql'
\ir 'ne_0402_match_georegp_peninsula.sql'
\ir 'ne_0403_match_georegp_lakegrp.sql'
\ir 'ne_0404_match_georegp_valley.sql'
\ir 'ne_0405_match_georegp_plain.sql'
\ir 'ne_0406_match_georegp_wetland.sql'
\ir 'ne_0407_match_georegp_plateau.sql'
\ir 'ne_0408_match_georegp_isthmus.sql'
\ir 'ne_0409_match_georegp_tundra.sql'
\ir 'ne_0410_match_georegp_basin.sql'
\ir 'ne_0411_match_georegp_pencape.sql'
\ir 'ne_0412_match_georegp_coast.sql'
\ir 'ne_0413_match_georegp_mountain.sql'



drop table if exists newd.ne_wd_match_geography_regions_polys_match_agg CASCADE;
create table         newd.ne_wd_match_geography_regions_polys_match_agg as
          select * from newd.ne_wd_georegp_continent_match_agg
union all select * from newd.ne_wd_georegp_delta_match_agg
union all select * from newd.ne_wd_georegp_island_match_agg
union all select * from newd.ne_wd_georegp_depression_match_agg
union all select * from newd.ne_wd_georegp_desert_match_agg
union all select * from newd.ne_wd_georegp_geoarea_match_agg
union all select * from newd.ne_wd_georegp_peninsula_match_agg
union all select * from newd.ne_wd_georegp_lakegrp_match_agg
union all select * from newd.ne_wd_georegp_valley_match_agg
union all select * from newd.ne_wd_georegp_plain_match_agg
union all select * from newd.ne_wd_georegp_wetland_match_agg
union all select * from newd.ne_wd_georegp_plateau_match_agg
union all select * from newd.ne_wd_georegp_isthmus_match_agg
union all select * from newd.ne_wd_georegp_tundra_match_agg
union all select * from newd.ne_wd_georegp_basin_match_agg
union all select * from newd.ne_wd_georegp_pencape_match_agg
union all select * from newd.ne_wd_georegp_coast_match_agg
union all select * from newd.ne_wd_georegp_mountain_match_agg
;


