

--
--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Delta           |  12 |
--+-----------------+-----
--


\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Delta''')
\set wd_filter          (a_wof_type  &&  ARRAY['''delta'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_delta
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        delta

\set safedistance       400000
\set searchdistance     600003
\set suggestiondistance 200000

\ir 'template_newd_geopoly.sql'

