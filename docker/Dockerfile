FROM debian:latest

RUN \
    apt-get update \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-suggests --no-install-recommends \
    ca-certificates \
    curl \
    gnupg2 \
    lsb-release \
    wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

WORKDIR /wof/tools

ENV PG_MAJOR 12

RUN echo "deb http://apt.postgresql.org/pub/repos/apt buster-pgdg main $PG_MAJOR" > /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
# Node
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -

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
        osmium-tool \
        parallel \
        pbzip2 \
        postgresql-client-${PG_MAJOR} \
        pspg \
        python-dev \
        python-gdal \
        python-pip \
        sqlite3 \
        sudo \
        wget \
    && rm -rf /var/lib/apt/lists/* /tmp/*

# Install Julia 
ENV JULIA_MAJOR=1.4
ENV JULIA_VERSION=1.4.0
ENV JULIA_SHA256=30d126dc3598f3cd0942de21cc38493658037ccc40eb0882b3b4c418770ca751

ENV JULIA_DIR=/usr/local/julia
ENV JULIA_PATH=${JULIA_DIR}

RUN mkdir ${JULIA_DIR} && \
    cd /tmp && \
    wget -q https://julialang-s3.julialang.org/bin/linux/x64/${JULIA_MAJOR}/julia-${JULIA_VERSION}-linux-x86_64.tar.gz && \
    echo "$JULIA_SHA256 julia-${JULIA_VERSION}-linux-x86_64.tar.gz" | sha256sum -c - && \
    tar xzf julia-${JULIA_VERSION}-linux-x86_64.tar.gz -C ${JULIA_DIR} --strip-components=1 && \
    rm /tmp/julia-${JULIA_VERSION}-linux-x86_64.tar.gz
RUN ln -fs ${JULIA_DIR}/bin/julia /usr/local/bin/julia

RUN julia -e 'using Pkg; Pkg.add(["PackageCompiler","CSV","DataFrames","DataStreams","JSON","LibPQ","SQLite","XLSX"]);Pkg.precompile()'
# Do Ahead of Time Compilation using PackageCompiler
RUN julia --trace-compile="traced.jl" \
          -e 'using CSV, DataFrames, DataStreams, JSON, LibPQ, SQLite, XLSX' \
    && julia -e 'using PackageCompiler; \
              PackageCompiler.create_sysimage([:CSV, :DataFrames, :DataStreams, :JSON, :LibPQ, :SQLite, :XLSX]; precompile_statements_file="traced.jl", replace_default=true) \
             ' \
    && rm traced.jl
# check;
RUN julia -e 'using CSV, DataFrames, DataStreams, JSON, LibPQ, SQLite, XLSX;' \
    && julia -e 'using InteractiveUtils; versioninfo()'



# Install GO
ARG GOPATH=/go
ENV GOPATH=${GOPATH} \
    PATH=/go/bin:/usr/local/go/bin:$PATH

ARG GOLANG_VERSION=1.14.2
ARG GOLANG_DOWNLOAD_SHA256=6272d6e940ecb71ea5636ddb5fab3933e087c1356173c61f4a803895e947ebb3

RUN set -eux \
    && apt-get update \
    && apt-get install -yqq --no-install-suggests --no-install-recommends \
    libc6-dev \
    make \
    unzip \
    && rm -rf /var/lib/apt/lists/* \
    \
    && curl -o go.tgz -sSL "https://golang.org/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz" \
    && echo "${GOLANG_DOWNLOAD_SHA256} *go.tgz" | sha256sum -c - \
    && tar -C /usr/local -xzf go.tgz \
    && rm go.tgz  \
    && mkdir ${GOPATH}

#  Install go utilities ; packages
RUN GO111MODULE=on go get -u github.com/urfave/cli@v1
RUN GO111MODULE=on go get -u github.com/lukasmartinelli/pgclimb

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

###   no such option: --process-dependency-links
RUN sudo -H pip install pip   # --upgrade
RUN sudo -H pip install setuptools wheel # --upgrade
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

RUN  git clone --quiet --depth 1 https://github.com/whosonfirst/py-mapzen-whosonfirst-export.git \
    && cd py-mapzen-whosonfirst-export \
    && pip install -r requirements.txt --process-dependency-links . \
    && python setup.py install

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
    && make tools

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-inspector.git \
    && cd go-whosonfirst-inspector \
    && make tools

RUN   git clone --quiet --depth 1 https://github.com/whosonfirst/go-whosonfirst-csv.git \
    && cd go-whosonfirst-csv \
    && make tools

RUN    git clone --quiet --depth 1 -b add_properties_column  https://github.com/ImreSamu/go-whosonfirst-pgis \
    && cd go-whosonfirst-pgis \
    && rm -Rf ./vendor/github.com/tidwall/pretty/ \
    && go get -v github.com/tidwall/pretty \
    && make dwindex

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

RUN sudo -H npm install --unsafe-perm -g wikidata-taxonomy

ADD  .psqlrc /root/.psqlrc


WORKDIR /wof/