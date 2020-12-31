ARG DOCKER_IMAGE=debian:bullseye
FROM ${DOCKER_IMAGE}
ARG DOCKER_IMAGE
ENV DOCKER_IMAGE=$DOCKER_IMAGE

ENV DEBIAN_FRONTEND noninteractive
ENV PG_MAJOR 13
ENV LANG en_US.UTF-8

RUN \
    apt-get update \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-suggests --no-install-recommends \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        gnupg2 \
        locales \
        lsb-release \
        tzdata \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

RUN lsb_release
RUN lsb_release -cs && lsb_release -cs

#  Add Postgres official Repo
RUN set -ex \
    && if [ "${DOCKER_IMAGE}" = "ubuntu:20.10" ] ; then \
        # no officical pg repo for ubuntu 20.10 ;
        echo "comment: using ubuntu:20.10 postgres !" ; \
    else \
        BASE_PG_REPO='bullseye' \
        && echo "deb http://apt.postgresql.org/pub/repos/apt/ ${BASE_PG_REPO}-pgdg main $PG_MAJOR" > /etc/apt/sources.list.d/pgdg.list \
        && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - ; \
    fi


WORKDIR /wof/tools


RUN \
    apt-get update \
    && apt-get install -yqq --no-install-suggests --no-install-recommends \
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
        libpq-dev \
        libprotobuf-dev \
        libsqlite3-dev \
        libtool \
        mc \
        nano \
        nodejs \
        npm \
        osmium-tool \
        parallel \
        pbzip2 \
        postgresql-client-${PG_MAJOR} \
        pspg \
        python3-dev \
        python3-gdal \
        python3-pip \
        sqlite3 \
        sudo \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN sudo -H npm install --unsafe-perm -g wikidata-taxonomy \
    && wdtaxonomy -V

# Install Julia
ENV JULIA_MAJOR=1.5
ENV JULIA_VERSION=1.5.3
ENV JULIA_SHA256=f190c938dd6fed97021953240523c9db448ec0a6760b574afd4e9924ab5615f1

ENV JULIA_DIR=/usr/local/julia
ENV JULIA_PATH=${JULIA_DIR}

ENV JULIA_DIR=/usr/local/julia
ENV JULIA_PATH=${JULIA_DIR}
RUN mkdir ${JULIA_DIR} && \
    cd /tmp && \
    wget -q https://julialang-s3.julialang.org/bin/linux/x64/${JULIA_MAJOR}/julia-${JULIA_VERSION}-linux-x86_64.tar.gz && \
    echo "$JULIA_SHA256 julia-${JULIA_VERSION}-linux-x86_64.tar.gz" | sha256sum -c - && \
    tar xzf julia-${JULIA_VERSION}-linux-x86_64.tar.gz -C ${JULIA_DIR} --strip-components=1 && \
    rm /tmp/julia-${JULIA_VERSION}-linux-x86_64.tar.gz
RUN ln -fs ${JULIA_DIR}/bin/julia /usr/local/bin/julia

RUN julia -e 'using Pkg; Pkg.add(["PackageCompiler","SystemBenchmark","Arrow","ClickHouse","CpuId","CSV","DataFrames","DataStreams","JSON","JSON3","LibPQ","SQLite","XLSX"]);Pkg.precompile()'
# Do Ahead of Time Compilation using PackageCompiler
RUN julia --trace-compile="traced.jl" \
          -e 'using CpuId, SystemBenchmark, Arrow, ClickHouse, CSV, DataFrames, DataStreams, JSON, JSON3, LibPQ, SQLite, XLSX' \
    && julia -e 'using PackageCompiler; \
              PackageCompiler.create_sysimage([:CpuId, :SystemBenchmark, :Arrow, :ClickHouse, :CSV, :DataFrames, :DataStreams, :JSON, :JSON3, :LibPQ, :SQLite, :XLSX]; \
                   precompile_statements_file="traced.jl", \
                   cpu_target = "native", \
                   replace_default=true) \
             ' \
    && rm traced.jl
# check;
RUN julia -e 'using CpuId, SystemBenchmark, Arrow, ClickHouse, CSV, DataFrames, DataStreams, JSON, LibPQ, SQLite, XLSX;' \
    && julia -e 'using InteractiveUtils; versioninfo()'

# Install GO
ARG GOPATH=/go
ENV GOPATH=${GOPATH} \
    PATH=/go/bin:/usr/local/go/bin:$PATH

ARG GOLANG_VERSION=1.15.6
ARG GOLANG_DOWNLOAD_SHA256=3918e6cc85e7eaaa6f859f1bdbaac772e7a825b0eb423c63d3ae68b21f84b844
RUN set -eux \
    && apt-get update \
    && apt-get install -yqq --no-install-suggests --no-install-recommends \
        libc6-dev \
        make \
        unzip \
    \
    && curl -o go.tgz -sSL "https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz" \
    && echo "${GOLANG_DOWNLOAD_SHA256} *go.tgz" | sha256sum -c - \
    && tar -C /usr/local -xzf go.tgz \
    && rm go.tgz  \
    && mkdir ${GOPATH} \
    \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/*

#  Install go utilities ; packages
RUN    GO111MODULE=on go get -u github.com/urfave/cli@v1 \
    && GO111MODULE=on go get -u github.com/lukasmartinelli/pgclimb

RUN \
       go get github.com/Code-Hex/pget/cmd/pget \
    && go get github.com/fd0/machma \
    && go get github.com/klauspost/compress \
    && go get github.com/klauspost/crc32 \
    && go get github.com/klauspost/pgzip/... \
    && go get github.com/miku/parallel \
    && go get github.com/mmcloughlin/geohash \
    && go get github.com/shenwei356/rush/ \
    && go get github.com/svent/sift \
    && go get github.com/tidwall/gjson \
    && go get github.com/lib/pq

#    && go get github.com/lukasmartinelli/pgfutter \
RUN    go get     github.com/omniscale/imposm3 \
    && go install github.com/omniscale/imposm3/cmd/imposm

#
RUN    pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir --upgrade setuptools wheel \
    && pip3 install --no-cache-dir --upgrade csvkit \
    && pip3 install --no-cache-dir --upgrade xlsx2csv

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

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-export.git \
    && cd py-mapzen-whosonfirst-export \
    && pip3 install --no-cache-dir -r requirements.txt . \
    && python3 setup.py install

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-search.git \
    && cd py-mapzen-whosonfirst-search \
    && pip3 install --no-cache-dir -r requirements.txt . \
    && python3 setup.py install

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-spatial.git \
    && cd py-mapzen-whosonfirst-spatial \
    && pip3 install --no-cache-dir -r requirements.txt . \
    && pip3 install --no-cache-dir psycopg2 shapely \
    && python3 setup.py install

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-utils.git \
    && cd py-mapzen-whosonfirst-utils \
    && pip3 install --no-cache-dir -r requirements.txt . \
    && python3 setup.py install

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-clone.git \
    && cd go-whosonfirst-clone \
    && make deps \
    && make bin

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-meta.git \
    && cd go-whosonfirst-meta \
    && make tools

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-inspector.git \
    && cd go-whosonfirst-inspector \
    && make cli

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-csv.git \
    && cd go-whosonfirst-csv \
    && make tools

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-dist.git \
    && cd go-whosonfirst-dist \
    && make tools

RUN git clone --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-properties.git

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-bundles.git  \
    && cd go-whosonfirst-bundles \
    && make tools

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-validate.git  \
    && cd go-whosonfirst-validate \
    && make tools

RUN git clone  --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-cookbook.git
RUN git clone  --quiet --depth 1 https://github.com/NikolayS/postgres_dba.git
RUN git clone  --quiet --depth 1 https://github.com/jfcoz/postgresqltuner.git \
    && chmod +x /wof/tools/postgresqltuner/postgresqltuner.pl

RUN git clone --depth 1 --branch master https://github.com/lesovsky/pgcenter \
    && cd pgcenter \
    && go get -u github.com/lib/pq@v1.8.0 \
    && make \
    && make install \
    && pgcenter --version

ADD  .psqlrc /root/.psqlrc


##RUN set -eux \
##    && apt-get update \
##    && apt-get install -yqq --no-install-suggests --no-install-recommends \
##    bc \
##    libedit-dev \
##    libpam-dev \
##    libpam0g-dev \
##    libpq-dev \
##    libpq5 \
##    libselinux1-dev \
##    libssl-dev \
##    openssl \
##    postgresql-client-common \
##    postgresql-common \
##    postgresql-server-dev-12 \
##    libkrb5-dev \
##    && rm -rf /var/lib/apt/lists/*
##
##RUN git clone  --quiet --depth 1 https://github.com/ossc-db/pg_bulkload.git \
##   && cd pg_bulkload \
##   && make -j 2 \
##   && make install

WORKDIR /wof/