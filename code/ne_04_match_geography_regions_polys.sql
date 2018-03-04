


\set neextrafields   ,namealt,region,subregion,min_label,max_label, scalerank, label, wdid_score

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
\ir 'ne_0414_match_georegp_fictional.sql'




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
union all select * from newd.ne_wd_georegp_fictional_match_agg
;



drop table if exists newd.ne_wd_match_geography_regions_polys_match_notfound CASCADE;
create table         newd.ne_wd_match_geography_regions_polys_match_notfound as
          select * from newd.ne_wd_georegp_continent_match_notfound
union all select * from newd.ne_wd_georegp_delta_match_notfound
union all select * from newd.ne_wd_georegp_island_match_notfound
union all select * from newd.ne_wd_georegp_depression_match_notfound
union all select * from newd.ne_wd_georegp_desert_match_notfound
union all select * from newd.ne_wd_georegp_geoarea_match_notfound
union all select * from newd.ne_wd_georegp_peninsula_match_notfound
union all select * from newd.ne_wd_georegp_lakegrp_match_notfound
union all select * from newd.ne_wd_georegp_valley_match_notfound
union all select * from newd.ne_wd_georegp_plain_match_notfound
union all select * from newd.ne_wd_georegp_wetland_match_notfound
union all select * from newd.ne_wd_georegp_plateau_match_notfound
union all select * from newd.ne_wd_georegp_isthmus_match_notfound
union all select * from newd.ne_wd_georegp_tundra_match_notfound
union all select * from newd.ne_wd_georegp_basin_match_notfound
union all select * from newd.ne_wd_georegp_pencape_match_notfound
union all select * from newd.ne_wd_georegp_coast_match_notfound
union all select * from newd.ne_wd_georegp_mountain_match_notfound
union all select * from newd.ne_wd_georegp_fictional_match_notfound
;
 --





\set ne_wd_match               newd.ne_wd_match_geography_regions_polys_match
\set ne_wd_match_agg           newd.ne_wd_match_geography_regions_polys_match_agg
\set ne_wd_match_agg_sum       newd.ne_wd_match_geography_regions_polys_match_agg_sum
\set ne_wd_match_notfound      newd.ne_wd_match_geography_regions_polys_match_notfound


drop table if exists  :ne_wd_match_agg_sum CASCADE;
CREATE UNLOGGED TABLE  :ne_wd_match_agg_sum  as
with
_matched as (
    select _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category, count(*) as N
    from :ne_wd_match_agg
    group by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla, wd_number_of_matches, _firstmatch_distance_category
),
_notfound as (
    select _matching_category,featurecla,  null::int as wd_number_of_matches, null::text as _firstmatch_distance_category, count(*) as N
    from :ne_wd_match_notfound
    group by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
    order by  _matching_category,featurecla,  wd_number_of_matches, _firstmatch_distance_category
)

           select * from _matched
union all  select * from _notfound
;

ANALYSE :ne_wd_match_agg_sum ;


\set _pct _pct
drop table if exists  :ne_wd_match_agg_sum:_pct CASCADE;
CREATE UNLOGGED TABLE          :ne_wd_match_agg_sum:_pct  as
with
 total AS (  select
            '-- total --' as _matching_category
           , sum(N) as N
           , null::numeric(10,5) as pct
           from :ne_wd_match_agg_sum )
,msum  as (
    SELECT _matching_category
        , sum(N)       as N
    FROM :ne_wd_match_agg_sum
    group by _matching_category
    order by _matching_category
)
select msum._matching_category
      ,msum.N
      ,((100.0 * msum.N ) /total.N)::numeric(10,5) as pct
from msum, total
union all
  select    *
  from total
;
ANALYSE :ne_wd_match_agg_sum:_pct;

select * from :ne_wd_match_agg_sum:_pct;