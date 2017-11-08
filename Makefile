.PHONY: all
SHELL := /bin/bash

all: build

init:
	mkdir -p ../postgres_data
	mkdir -p ../whosonfirst-data
	mkdir -p ../wikidata_dump
	mkdir -p ./log

build:
	cd ./docker && docker build -t wof_wiki_dw  . && cd ..
	docker images | grep  wof_wiki_dw

dev:
	docker-compose run --rm  wof_wiki_dw /bin/bash

down:
	docker-compose down

up:
	docker-compose up db -d
