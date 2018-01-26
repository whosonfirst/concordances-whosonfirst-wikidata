


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Depression      |  2  |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Depression''')
\set wd_filter          (a_wof_type  &&  ARRAY['''depression'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_depression
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        depression

\set safedistance       400000
\set searchdistance     900003
\set suggestiondistance 400000

\ir 'template_newd_geopoly.sql'

