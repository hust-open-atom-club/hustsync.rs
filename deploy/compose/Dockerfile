# hustsync and hustsynctl built from this repository.
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p hustsync -p hustsynctl

FROM alpine:3.23
RUN apk add --no-cache ca-certificates docker-cli rsync
COPY --from=build /src/target/release/hustsync /src/target/release/hustsynctl /usr/local/bin/
RUN ln -s hustsynctl /usr/local/bin/tunasynctl
