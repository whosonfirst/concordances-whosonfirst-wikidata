.PHONY: all
SHELL := /bin/bash
PWD:=$(shell pwd)

all: init build

init:
	mkdir -p ../postgres_data
	mkdir -p ../whosonfirst-data
	mkdir -p ../wikidata_dump
	mkdir -p ../natural-earth-vector
	mkdir -p ../geonames
	mkdir -p /var/www/html/wof-wiki-dw/output
	docker -v
	docker-compose -v

build:
	cd ./docker && docker build -t wof_wiki_dw -f Dockerfile            . && cd ..
	cd ./docker && docker build -t wof_postgis -f Dockerfile_postgis    . && cd ..
#	cd ./docker && docker build -t wof_jupyter -f Dockerfile_jupyter    . && cd ..
	docker images | grep  wof_

dev:
	docker-compose run --rm  wof_wiki_dw /bin/bash

down:
	docker-compose exec db gosu postgres bash -c "pg_ctl --mode=fast --wait stop || :" || :
	docker-compose down --timeout 60

dbtest:
	docker-compose exec db pg_test_fsync
	docker-compose exec db pg_test_timing

dbstatus:
	docker-compose exec db gosu postgres pg_ctl status

up:
	docker-compose up  -d

download_inputs:
	time docker-compose run --rm  wof_wiki_dw /wof/code/job_download_inputs.sh

update_wof:
	time docker-compose run --rm  wof_wiki_dw /wof/code/job_update_wof.sh

run:
	nohup docker-compose run --rm -T wof_wiki_dw /wof/code/job.sh &

run_ne:
	nohup docker-compose run --rm -T wof_wiki_dw /wof/code/job_ne_wd.sh &

listsize:
	du -sh ../*

cleandb:
	docker-compose down
	docker run --rm -it -v $(PWD)/../postgres_data:/var/lib/postgresql/data wof_postgis bash -c "ls -la /var/lib/postgresql/data"
	docker run --rm -it -v $(PWD)/../postgres_data:/var/lib/postgresql/data wof_postgis bash -c "rm -rf /var/lib/postgresql/data/*"

speedtest:
	docker-compose down
	docker run --rm -it -v $(PWD)/../postgres_data:/var/lib/postgresql/data wof_postgis bash -c "ls -la /var/lib/postgresql/data"
	docker run --rm -it -v $(PWD)/../postgres_data:/var/lib/postgresql/data wof_postgis bash -c "rm -rf /var/lib/postgresql/data/*"
	docker-compose up  -d
	nohup docker-compose run --rm -T wof_wiki_dw /wof/code/job_ne_wd.sh &
