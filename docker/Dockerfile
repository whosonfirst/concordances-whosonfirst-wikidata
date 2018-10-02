FROM golang:latest

WORKDIR /wof/tools

ENV PG_MAJOR 11

# https://github.com/docker-library/postgres/issues/484 (clang-6.0 required, only available in stretch-backports)
RUN sh -c 'echo "deb http://deb.debian.org/debian stretch-backports main" > /etc/apt/sources.list.d/pgdg.list;'
# PostgreSQL
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main $PG_MAJOR" >> /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
# Git-fs
RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash
# Node
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -

RUN \
       apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        bison \
        build-essential \
        curl \
        flex \
        gdal-bin \
        git \
        git-lfs \
        jq \
        libdbd-pg-perl \
        libgeos-dev \
        libleveldb-dev \
        libprotobuf-dev \
        libtool \
        mc \
        nano \
        nodejs \
        osmium-tool \
        parallel \
        pbzip2 \
        postgresql-client-${PG_MAJOR} \
        pspg \
        python-pip \
        sqlite3 \
        sudo \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN \
       go get github.com/Code-Hex/pget/cmd/pget \
    && go get github.com/fd0/machma \
    && go get github.com/klauspost/compress \
    && go get github.com/klauspost/crc32 \
    && go get github.com/klauspost/pgzip/... \
    && go get github.com/lukasmartinelli/pgclimb \
    && go get github.com/lukasmartinelli/pgfutter \
    && go get github.com/miku/parallel \
    && go get github.com/mmcloughlin/geohash \
    && go get github.com/shenwei356/rush/ \
    && go get github.com/svent/sift \
    && go get github.com/tidwall/gjson

RUN    go get github.com/omniscale/imposm3 \
    && go install github.com/omniscale/imposm3/cmd/imposm

RUN sudo -H pip install pip --upgrade
RUN sudo -H pip install setuptools wheel --upgrade
RUN sudo -H pip install csvkit
RUN sudo -H pip install xlsx2csv

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

RUN git clone  --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-cookbook.git
RUN git clone  --quiet --depth 1 https://github.com/NikolayS/postgres_dba.git
RUN git clone  --quiet --depth 1 https://github.com/jfcoz/postgresqltuner.git
RUN chmod +x /wof/tools/postgresqltuner/postgresqltuner.pl

RUN sudo -H npm install --unsafe-perm -g wikidata-taxonomy

ADD  .psqlrc /root/.psqlrc

ENV JULIA_MAJOR=1.0
ENV JULIA_VERSION=1.0.1
ENV JULIA_SHA256=9ffbcf7f4a111e13415954caccdd1ce90b5c835cee9f62d6ac708f5b752c87dd

ENV JULIA_DIR=/usr/local/julia
ENV JULIA_PATH=${JULIA_DIR}

RUN mkdir ${JULIA_DIR} && \
    cd /tmp && \
    wget -q https://julialang-s3.julialang.org/bin/linux/x64/${JULIA_MAJOR}/julia-${JULIA_VERSION}-linux-x86_64.tar.gz && \
    echo "$JULIA_SHA256 julia-${JULIA_VERSION}-linux-x86_64.tar.gz" | sha256sum -c - && \
    tar xzf julia-${JULIA_VERSION}-linux-x86_64.tar.gz -C ${JULIA_DIR} --strip-components=1 && \
    rm /tmp/julia-${JULIA_VERSION}-linux-x86_64.tar.gz
RUN ln -fs ${JULIA_DIR}/bin/julia /usr/local/bin/julia

WORKDIR /wof/

RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add CSV        ; precompile"); using CSV'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add DataFrames ; precompile"); using DataFrames'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add DataStreams; precompile"); using DataStreams'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add JSON       ; precompile"); using JSON'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add LibPQ      ; precompile"); using LibPQ'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add SQLite     ; precompile"); using SQLite'
RUN julia -O3 -e 'using Pkg; Pkg.REPLMode.pkgstr("add XLSX       ; precompile"); using XLSX'
