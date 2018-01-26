



--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Geoarea         |  43 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Geoarea''')
\set wd_filter          (a_wof_type  &&  ARRAY['''region''','''peninsula'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_geoarea
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        region

\set safedistance       400000
\set searchdistance     900003
\set suggestiondistance 400000

\ir 'template_newd_geopoly.sql'



