


--+-----------------+-----+
--|   featurecla    |  n  |
--+-----------------+-----+
--| Range/mtn       | 219 |
--| Foothills       |   3 | ?
--+-----------------+-----

\set wd_input_table     newd.wd_match_geography_regions_polys
\set ne_input_table     newd.ne_match_geography_regions_polys
\set ne_filter          (featurecla )   in    ('''Range/mtn''','''Foothills''')
\set wd_filter          (a_wof_type  &&  ARRAY['''mountain'''] )  and (a_wof_type  @>  ARRAY['''hasP625'''] ) 

\set mgrpid             georegp_mountain
\set wd_input_table     newd.wd_:mgrpid
\set ne_input_table     newd.ne_:mgrpid

\set words2clean        mountain|mountains|gora|mt.|mtn.|mts.|vulkan|volcano|mount|mont|monte|peak|pk.|pico|gunung|cerro|range|ra.

\set safedistance        800000
\set searchdistance     3900003
\set suggestiondistance  800000

\ir 'template_newd_geopoly.sql'

