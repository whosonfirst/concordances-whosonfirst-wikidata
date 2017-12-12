#!/bin/sh
set -e


# Quick setup for Ubuntu
#   Install directory :  /mnt/data/wof
#   


WOF-WIKI-DW-DIR=/mnt/data/wof
WOF-WIKI-DW-GIT-URL=https://github.com/ImreSamu/wof-wiki-dw.git


apt-get update
apt-get install -y \
    apache2 \
    apt-transport-https \
    ca-certificates \
    curl \
    git \
    mc \
    mdadm \
    software-properties-common \
    sudo \
    wget


# Install Docker & Docker-compose
curl -sSL https://get.docker.com/ | sh
curl -L https://github.com/docker/compose/releases/download/1.17.1/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose


# install latest version of application
mkdir -p /var/www/html/wof-wiki-dw/output
mkdir -p ${WOF-WIKI-DW-DIR}
cd ${WOF-WIKI-DW-DIR}
git clone --depth 1 ${WOF-WIKI-DW-GIT-URL}
cd wof-wiki-dw


# Build docker images
make build


# Create directories 
make init




