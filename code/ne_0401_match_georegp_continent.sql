--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Continent       |   7 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Continent''')
\set wd_filter          (a_wof_type  &&  ARRAY['''continent'''] )  

\set mgrpid             georegp_continent
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        continent

\set safedistance       999990000
\set searchdistance     999990003
\set suggestiondistance 999990000

\ir 'template_newd_geopoly.sql'


