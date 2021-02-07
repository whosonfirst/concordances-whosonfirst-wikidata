ARG DOCKER_IMAGE=debian:bullseye
FROM ${DOCKER_IMAGE}
ARG DOCKER_IMAGE
ENV DOCKER_IMAGE=$DOCKER_IMAGE

ENV DEBIAN_FRONTEND noninteractive
ENV PG_MAJOR 13

ENV LC_ALL en_US.UTF-8
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
        software-properties-common \
        tzdata \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

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


# Add R Repo
RUN apt-key adv \
        --keyserver keys.gnupg.net \
        --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'

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
        libgdal-dev \
        libgeos-dev \
        libleveldb-dev \
        libpq-dev \
        libproj-dev \
        libprotobuf-dev \
        libsqlite3-dev \
        libtool \
        libudunits2-dev \
        mc \
        nano \
        nodejs \
        npm \
        osmium-tool \
        parallel \
        pbzip2 \
        pigz \
        postgresql-client-${PG_MAJOR} \
        pspg \
        python3-dev \
        python3-gdal \
        python3-pip \
        sqlite3 \
        sudo \
        time \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*


# Install R.
RUN \
    apt-get update \
    && apt-get install -yqq \
        libcairo2-dev \
        littler \
        pandoc \
        r-base \
        r-base-dev \
        r-base-html \
        r-recommended \
    && rm -rf /var/lib/apt/lists/* /tmp/*

RUN Rscript -e 'install.packages(c("sf", "tidyverse", "WikidataR"), dependencies = TRUE, repos = "https://cloud.r-project.org")'
# R smoke test
RUN Rscript -e 'library(sf);library(WikidataR);library(tidyverse);print("R - is working!")'

RUN sudo -H npm install --unsafe-perm -g wikidata-taxonomy \
    && npm cache clean --force \
    && wdtaxonomy -V

# Install Julia 1.6 beta1
ENV JULIA_MAJOR=1.6
ENV JULIA_VERSION=1.6.0-rc1
ENV JULIA_SHA256=72847bd2b4d4db9d5970512f79fd5a7f76ad56cf0f2ea5a80eff032364b19c8b 
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
# julia -  smoke test
RUN julia -e 'using CpuId, SystemBenchmark, Arrow, ClickHouse, CSV, DataFrames, DataStreams, JSON, LibPQ, SQLite, XLSX;' \
    && julia -e 'using InteractiveUtils; versioninfo()'

# Install GO
ARG GOPATH=/go
ENV GOPATH=${GOPATH} \
    PATH=${GOPATH}/bin:/usr/local/go/bin:$PATH
ARG GOLANG_VERSION=1.16rc1
ARG GOLANG_DOWNLOAD_SHA256=6a62610f56a04bae8702cd2bd73bfea34645c1b89ded3f0b81a841393b6f1f14
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
    && mkdir -p "$GOPATH/src" "$GOPATH/bin" \
    && chmod -R 777 "$GOPATH" \
    \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/*

#  Install go utilities ; packages
RUN    GO111MODULE=on go get -u github.com/urfave/cli@v1 \
    && GO111MODULE=on go get -u github.com/lukasmartinelli/pgclimb

RUN \
       GO111MODULE=off go get -u github.com/Code-Hex/pget/cmd/pget \
    && GO111MODULE=off go get -u github.com/fd0/machma \
    && GO111MODULE=off go get -u github.com/klauspost/compress \
    && GO111MODULE=off go get -u github.com/klauspost/crc32 \
    && GO111MODULE=off go get -u github.com/klauspost/pgzip/... \
    && GO111MODULE=off go get -u github.com/miku/parallel \
    && GO111MODULE=off go get -u github.com/mmcloughlin/geohash \
    && GO111MODULE=off go get -u github.com/shenwei356/rush/ \
    && GO111MODULE=off go get -u github.com/svent/sift \
    && GO111MODULE=off go get -u github.com/tidwall/gjson \
    && GO111MODULE=off go get -u github.com/lib/pq

#    && go get github.com/lukasmartinelli/pgfutter \
RUN    go get -u  github.com/omniscale/imposm3@master \
    && go install github.com/omniscale/imposm3/cmd/imposm@master

#
RUN    pip3 install --no-cache-dir --upgrade pip \
    && pip3 install --no-cache-dir --upgrade setuptools \
    && pip3 install --no-cache-dir --upgrade wheel \
    && pip3 install --no-cache-dir --upgrade csvkit \
    && pip3 install --no-cache-dir --upgrade xlsx2csv \
    && pip3 install --no-cache-dir --upgrade duckdb


RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-utils.git \
    && cd go-whosonfirst-utils \
    && GO111MODULE=auto make bin

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-s3.git \
    && cd go-whosonfirst-s3 \
    && GO111MODULE=auto make bin

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-concordances.git \
    && cd go-whosonfirst-concordances \
    && GO111MODULE=auto make bin

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
    && GO111MODULE=auto make deps \
    && GO111MODULE=auto make bin

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-meta.git \
    && cd go-whosonfirst-meta \
    && GO111MODULE=auto make tools

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-inspector.git \
    && cd go-whosonfirst-inspector \
    && GO111MODULE=auto make cli

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-csv.git \
    && cd go-whosonfirst-csv \
    && GO111MODULE=auto make tools

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-dist.git \
    && cd go-whosonfirst-dist \
    && GO111MODULE=auto make tools

RUN git clone --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-properties.git

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-bundles.git  \
    && cd go-whosonfirst-bundles \
    && GO111MODULE=auto make tools

RUN    git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-validate.git  \
    && cd go-whosonfirst-validate \
    && GO111MODULE=auto make tools

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-export.git \
    && cd go-whosonfirst-export \
    && GO111MODULE=auto make tools

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-index.git \
    && cd go-whosonfirst-index \
    && GO111MODULE=auto make tools

RUN git clone --quiet --depth 1 https://github.com/whosonfirst/whosonfirst-cookbook.git
RUN git clone --quiet --depth 1 https://github.com/NikolayS/postgres_dba.git
RUN git clone --quiet --depth 1 https://github.com/jfcoz/postgresqltuner.git \
    && chmod +x /wof/tools/postgresqltuner/postgresqltuner.pl

RUN git clone --depth 1 --branch master https://github.com/lesovsky/pgcenter \
    && cd pgcenter \
    && go get -u github.com/lib/pq@v1.9.0 \
    && make \
    && make install \
    && pgcenter --version

ADD .psqlrc /root/.psqlrc

WORKDIR /wof/