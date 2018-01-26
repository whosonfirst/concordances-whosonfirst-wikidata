


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Desert          |  58 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Desert''')
\set wd_filter          (a_wof_type  &&  ARRAY['''desert''','''playa'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_desert
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        desert

\set safedistance       400000
\set searchdistance     900003
\set suggestiondistance 400000

\ir 'template_newd_geopoly.sql'

