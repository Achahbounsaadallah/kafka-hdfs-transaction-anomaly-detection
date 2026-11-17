
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

RUN printf '%s\n' \
    'deb http://archive.debian.org/debian stretch main' \
    'deb http://archive.debian.org/debian-security stretch/updates main' \
    > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*

