
--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Island          | 292 |
--| Island group    | 158 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Island''','''Island group''')
\set wd_filter          (a_wof_type  &&  ARRAY['''island''','''archipelago'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_island
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        island|archipelago

\set safedistance       100000
\set searchdistance     400003
\set suggestiondistance  80000

\ir 'template_newd_geopoly.sql'



