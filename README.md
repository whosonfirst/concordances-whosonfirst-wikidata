# wof-wiki-dw

Work in progress ...


Who's On First + wikidata   -> PostgreSQL QA DW  




## System requirements:

*  clean Ubuntu/Debian 
*  ~200GB free disk space ( SSD ) (  default dir: /mnt/data/wof )
*  strong intel CPU

## Setup

for Ubuntu ( with default install dir: /mnt/data/wof ) run this script

```bash
./install/setup.sh
```

## Run
```

# Download input files  ( wikidata JSON Dump,  wof data, ... )
make download_inputs

# Load to Postgres and Processing
make run

```