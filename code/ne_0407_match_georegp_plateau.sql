


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Plateau         |  71 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Plateau''')
\set wd_filter          (a_wof_type  &&  ARRAY['''plateau''','''mountain'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_plateau
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        plateau|plat.

\set safedistance       400000
\set searchdistance     900003
\set suggestiondistance 100000

\ir 'template_newd_geopoly.sql'

