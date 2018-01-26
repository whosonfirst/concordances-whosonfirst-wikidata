


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Lake            |   3 |
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Lake''')
\set wd_filter          (a_wof_type  &&  ARRAY['''lakegrp'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_lakegrp
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        lake|lakes

\set safedistance       400000
\set searchdistance     900003
\set suggestiondistance 100000

\ir 'template_newd_geopoly.sql'

