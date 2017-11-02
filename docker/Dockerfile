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
        nano \
        postgresql-client-10 \
        sudo \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/* 

RUN \
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash \
    && apt-get install -y git-lfs  \
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
    && go get github.com/svent/sift \
    && go get github.com/Code-Hex/pget/cmd/pget

RUN    git clone --quiet --depth 1 -b add_properties_column  https://github.com/ImreSamu/go-whosonfirst-pgis \
    && cd go-whosonfirst-pgis \
    && make dwindex
 
RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-utils.git \
    && cd go-whosonfirst-utils \
    && make bin  

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-s3.git \
    && cd go-whosonfirst-s3 \
    && make bin

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-concordances.git \
    && cd go-whosonfirst-concordances \
    && make bin 

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-data-utils.git \
    && cd whosonfirst-data-utils

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/git-whosonfirst-data.git \
    && cd git-whosonfirst-data 

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-search.git 
RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-spatial






