.PHONY: all
SHELL := /bin/bash

all: init build

init:
	mkdir -p ../postgres_data
	mkdir -p ../whosonfirst-data
	mkdir -p ../wikidata_dump
	mkdir -p ./log
	docker -v
	docker-compose -v
	
build:
	cd ./docker && docker build -t wof_wiki_dw  . && cd ..
	docker images | grep  wof_wiki_dw

dev:
	docker-compose run --rm  wof_wiki_dw /bin/bash

down:
	docker-compose down

up:
	docker-compose up db -d

run:
	docker-compose run --rm  wof_wiki_dw /wof/code/job.sh