FROM golang:latest

WORKDIR /wof

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN \
       apt-get update \
    && apt-get install -y \
        autoconf \
        bison \
        build-essential \
        curl \
        flex \
        git \
        libtool \
        mc \
        postgresql-client-10 \
        sudo \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/* 

# Build jq from source : https://github.com/stedolan/jq
RUN \
    git clone git://github.com/stedolan/jq.git \
    && cd jq \
    && git submodule update --init \
    && autoreconf -fi \
    && ./configure --with-oniguruma=builtin \
    && make -j4  \
    && make install \
    && rm -rf /tmp/*
    
RUN \ 
    go get github.com/lukasmartinelli/pgfutter \
    && go get github.com/svent/sift 

RUN    git clone --quiet --depth 1 -b add_properties_column  https://github.com/ImreSamu/go-whosonfirst-pgis \
    && cd go-whosonfirst-pgis \
    && make dwindex
 