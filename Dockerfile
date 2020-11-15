FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && \
    apt install -y apt-utils gcc libzmq3-dev postgresql postgresql-common postgresql-client postgresql-contrib build-essential glances htop vim tree curl libpq-dev && \
    apt clean

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /home/rustup.sh && \
    chmod +x /home/rustup.sh && \
    /home/rustup.sh -y && \
    . $HOME/.cargo/env && \
    echo ". $HOME/.cargo/env" >> $HOME/.shrc

RUN mkdir -p /home/app && \
    echo "TEST_DATABASE_URL=postgres://postgres:postgres@localhost/hetnetdb" > /home/app/.env && \
    echo "AUTH_SECRET=ssuperspecialtestenvironmentsecretsuperspecialtestenvironmentsecretuperspecialtestenvironmentsecret" >> /home/app/.env && \
    echo "RUST_LOG=trace" >> /home/app/.env

RUN . $HOME/.shrc && \
    cargo install diesel_cli --no-default-features --features postgres

COPY migrations /home/app/migrations/
COPY src /home/app/src/
COPY diesel.toml Cargo.toml Cargo.lock /home/app/

RUN . $HOME/.shrc && \
    pg_ctlcluster 12 main start -- -t 300 && \
    su - postgres -c "psql -U postgres -c \"alter user postgres with password 'postgres';\"" && \
    cd /home/app && \
    diesel setup --database-url postgres://postgres:postgres@localhost/hetnetdb

ARG CARGO_FLAGS
RUN cd /home/app && . $HOME/.shrc && \
    cargo build ${CARGO_FLAGS} && \
    pg_ctlcluster 12 main start -- -t 300 && \
    cargo test ${CARGO_FLAGS} && \
    echo SUCCESS

