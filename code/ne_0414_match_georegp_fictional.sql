


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Dragons-be-here |   1 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Dragons-be-here''')
\set wd_filter          (a_wof_type  &&  ARRAY['''fictional'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_fictional
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        fictional

\set safedistance        800000
\set searchdistance     3900003
\set suggestiondistance  800000

\ir 'template_newd_geopoly.sql'

