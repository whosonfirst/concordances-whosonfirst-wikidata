.PHONY: all

all: build

init:
	mkdir -p ../postgres_data
	mkdir -p ../whosonfirst-data
	mkdir -p ../wikidata_dump

init-wofdata:
	pushd ..
	git clone --depth 1 https://github.com/whosonfirst-data/whosonfirst-data.git
	popd 

init-wikidata:
	pushd ..
	mkdir -p wikidata_dump
	cd wikidata_dump
	rm -f latest-all.json.bz2
	wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
	popd

build:
	cd ./docker && docker build -t wof_wiki_dw  . && cd ..
	docker images | grep  wof_wiki_dw

dev:
	docker-compose run --rm  wof_wiki_dw /bin/bash

down:
	docker-compose down

up:
	docker-compose up db -d
