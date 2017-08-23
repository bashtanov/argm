FROM debian:8.8

RUN apt-get update && apt-get install -y apt-utils && \
    apt-get install -y build-essential debhelper git && \
    echo 'deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main' > /etc/apt/sources.list.d/pgdg.list && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update && apt-get install -y postgresql-common postgresql-server-dev-all

WORKDIR "/mnt/build"
