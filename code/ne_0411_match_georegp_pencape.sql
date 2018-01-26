


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Pen/cape        |  56 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Pen/cape''')
\set wd_filter          (a_wof_type  &&  ARRAY['''cape''','''peninsula'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_pencape
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        peninsula

\set safedistance        800000
\set searchdistance     3900003
\set suggestiondistance  800000

\ir 'template_newd_geopoly.sql'

