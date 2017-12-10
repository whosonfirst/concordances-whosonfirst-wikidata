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
	cd ./docker && docker build -t wof_wiki_dw -f Dockerfile         . && cd ..
	cd ./docker && docker build -t wof_postgis -f Dockerfile_postgis . && cd ..
	docker images | grep  wof_

dev:
	docker-compose run --rm  wof_wiki_dw /bin/bash

down:
	docker-compose down

up:
	docker-compose up  -d

download_inputs:
	time docker-compose run --rm  wof_wiki_dw /wof/code/job_download_inputs.sh

update_wof:
	time docker-compose run --rm  wof_wiki_dw /wof/code/job_update_wof.sh

run:
	time docker-compose run --rm  wof_wiki_dw /wof/code/job.sh

test:
	time docker-compose run --rm  wof_wiki_dw /wof/code/rjob.sh

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
	time docker-compose run --rm  wof_wiki_dw /wof/code/job.sh