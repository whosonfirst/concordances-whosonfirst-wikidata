FROM golang:latest

WORKDIR /wof/tools

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
        gdal-bin \
        git \
        libtool \
        mc \
        nano \
        osmium-tool \
        parallel \
        pbzip2 \
        postgresql-client-10 \
        pspg \
        python-pip \
        sqlite3 \
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
       go get github.com/Code-Hex/pget/cmd/pget \
    && go get github.com/lukasmartinelli/pgclimb \
    && go get github.com/lukasmartinelli/pgfutter \
    && go get github.com/miku/parallel \
    && go get github.com/svent/sift \
    && go get github.com/tidwall/gjson

RUN    go get github.com/shenwei356/rush/ \
    && go get github.com/fd0/machma \
    && go get github.com/klauspost/compress \
    && go get github.com/klauspost/crc32 \
    && go get github.com/klauspost/pgzip/... 


RUN pip install csvkit


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

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-search.git \
    && cd py-mapzen-whosonfirst-search \
    && pip install -r requirements.txt . \
    && python setup.py install 

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-spatial.git \
    && cd py-mapzen-whosonfirst-spatial \
    && pip install -r requirements.txt . \
    && pip install psycopg2 shapely \
    && python setup.py install 

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-utils.git \
    && cd py-mapzen-whosonfirst-utils \
    && pip install -r requirements.txt . \
    && python setup.py install

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-clone.git \
    && cd go-whosonfirst-clone \
    && make deps \
    && make bin

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-meta.git \
    && cd go-whosonfirst-meta \
    && make bin

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-inspector.git \
    && cd go-whosonfirst-inspector \
    && make bin

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-csv.git \
    && cd go-whosonfirst-csv \
    && make build

RUN    git clone --quiet --depth 1 -b add_properties_column  https://github.com/ImreSamu/go-whosonfirst-pgis \
    && cd go-whosonfirst-pgis \
    && make dwindex
 
RUN git clone --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-properties.git

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-bundles.git  \
    && cd go-whosonfirst-bundles \
    && make bin

RUN    git clone --quiet --depth 1 https://github.com/gbb/par_psql.git  \
    && cd par_psql \
    && ./install.sh

RUN  git clone  --quiet --depth 1 https://github.com/NikolayS/postgres_dba.git

ADD  .psqlrc /root/.psqlrc

RUN \
       apt-get update \
    && apt-get install -y \
        libdbd-pg-perl \
    && rm -rf /var/lib/apt/lists/* /tmp/* 

RUN git clone  --quiet --depth 1 https://github.com/jfcoz/postgresqltuner.git 
RUN chmod +x /wof/tools/postgresqltuner/postgresqltuner.pl
WORKDIR /wof/