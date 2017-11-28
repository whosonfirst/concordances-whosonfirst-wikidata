/wof/code/cmd_load_wdplace.sh  0
/wof/code/cmd_load_wdplace.sh  1
/wof/code/cmd_load_wdplace.sh  2
/wof/code/cmd_load_wdplace.sh  3
/wof/code/cmd_load_wdplace.sh  4
/wof/code/cmd_load_wdplace.sh  5
/wof/code/cmd_load_wdplace.sh  6
/wof/code/cmd_load_wdplace.sh  7
pgfutter --schema wdplace --table wd_country    --jsonb  json /wof/wikidata_dump/wdplace_country.json     
pgfutter --schema wdplace --table wd_dependency --jsonb  json /wof/wikidata_dump/wdplace_dependency.json  
pgfutter --schema wdplace --table wd_region     --jsonb  json /wof/wikidata_dump/wdplace_region.json      
pgfutter --schema wdplace --table wd_county     --jsonb  json /wof/wikidata_dump/wdplace_county.json