


mkdir -p /wof/test/whosonfirst-data/data/
mkdir -p /wof/test/whosonfirst-data/meta/


function sample_wof(){
    metacsv=$1
    csvgrep -c wof_country -m AR  /wof/whosonfirst-data/meta/$metacsv  > /wof/test/whosonfirst-data/$metacsv
    wc -l /wof/test/whosonfirst-data/$metacsv

    /wof/go-whosonfirst-clone/bin/wof-clone-metafiles \
        -dest /wof/test/whosonfirst-data/data/ \
        -source file:///wof/whosonfirst-data/data/  \
        /wof/test/whosonfirst-data/$metacsv

    # create an empty csv header 
    head -n 1 /wof/whosonfirst-data/meta/$metacsv  >   /wof/test/whosonfirst-data/meta/$metacsv

}


sample_wof    wof-borough-latest.csv
sample_wof    wof-country-latest.csv
sample_wof    wof-empire-latest.csv
sample_wof    wof-macrohood-latest.csv

sample_wof    wof-neighbourhood-latest.csv
sample_wof    wof-timezone-latest.csv
sample_wof    wof-campus-latest.csv
sample_wof    wof-county-latest.csv
sample_wof    wof-localadmin-latest.csv
sample_wof    wof-macroregion-latest.csv
sample_wof    wof-ocean-latest.csv

sample_wof    wof-dependency-latest.csv
sample_wof    wof-locality-latest.csv 
sample_wof    wof-marinearea-latest.csv
sample_wof    wof-planet-latest.csv
sample_wof    wof-continent-latest.csv
sample_wof    wof-disputed-latest.csv
sample_wof    wof-macrocounty-latest.csv
sample_wof    wof-microhood-latest.csv
sample_wof    wof-region-latest.csv

ls -la /wof/test/whosonfirst-data/data/
/wof/go-whosonfirst-meta/bin/wof-build-metafiles -repo /wof/test/whosonfirst-data/
ls -la /wof/test/whosonfirst-data/meta/*

