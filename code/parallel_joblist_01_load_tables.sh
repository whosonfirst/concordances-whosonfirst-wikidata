/wof/code/21_import_wdplace.sh
/wof/code/20_import_wikidata.sh
/wof/code/23_import_wdlabels.sh
/wof/code/cmd_load_wof.sh      wf.wof_locality        wof-locality-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_localadmin      wof-localadmin-latest.csv
go run /wof/code/wdplaceparse.go /wof/code/wikidata_country.csv    > /wof/wikidata_dump/wdplace_country.json
go run /wof/code/wdplaceparse.go /wof/code/wikidata_dependency.csv > /wof/wikidata_dump/wdplace_dependency.json
go run /wof/code/wdplaceparse.go /wof/code/wikidata_region.csv     > /wof/wikidata_dump/wdplace_region.json
go run /wof/code/wdplaceparse.go /wof/code/wikidata_county.csv     > /wof/wikidata_dump/wdplace_county.json
/wof/code/cmd_load_wof.sh      wf.wof_neighbourhood   wof-neighbourhood-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_county          wof-county-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_campus          wof-campus-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_region          wof-region-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_microhood       wof-microhood-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_macrohood       wof-macrohood-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_macrocounty     wof-macrocounty-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_timezone        wof-timezone-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_marinearea      wof-marinearea-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_country         wof-country-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_empire          wof-empire-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_borough         wof-borough-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_macroregion     wof-macroregion-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_ocean           wof-ocean-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_dependency      wof-dependency-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_planet          wof-planet-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_continent       wof-continent-latest.csv
/wof/code/cmd_load_wof.sh      wf.wof_disputed        wof-disputed-latest.csv
/wof/code/01_load_iso_language_codes.sh
/wof/code/22_import_natural_earth.sh