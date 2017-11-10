.PHONY: all
SHELL := /bin/bash

all: init build

init:
	mkdir -p ../postgres_data
	mkdir -p ../whosonfirst-data
	mkdir -p ../wikidata_dump
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

listsize:
	du -sh ../*
