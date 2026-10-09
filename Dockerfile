# blocklyd as hosts run it, and as each release publishes it. Alpine's Rust targets musl, so the
# binary is static and runs on any Linux host. The final image is nothing but the release's files:
#   /blocklyd                 the binary (its entrypoint: docker run <image> --version)
#   /openapi/*.openapi.json   the wire types, which the control plane generates its types from
#   /deploy/                  Docker's daemon.json and the systemd unit
#   /LICENSE.md               FSL-1.1-ALv2
# Cubepals' control plane image copies them out of it, pinned by digest.
# The Rust is rust-toolchain.toml's, which isn't copied in: the tag says the same version, and CI
# fails if they differ. It comes from AWS's mirror of Docker's official images: Docker Hub caps
# anonymous pulls per IP, and CI runners share IPs.
FROM public.ecr.aws/docker/library/rust:1.94.1-alpine3.22 AS build
RUN apk add --no-cache musl-dev file
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
# The unit `blocklyd join` installs, and the daemon.json `doctor` checks against, are compiled in.
COPY deploy deploy
RUN cargo build --locked --release \
 && ./target/release/blocklyd --version \
 && file target/release/blocklyd | grep -q 'static' || { file target/release/blocklyd; exit 1; }

FROM scratch
COPY --from=build /src/target/release/blocklyd /blocklyd
COPY openapi /openapi
COPY deploy/daemon.json deploy/blocklyd.service /deploy/
COPY LICENSE.md /LICENSE.md
LABEL org.opencontainers.image.source=https://github.com/cubepals/blocklyd \
      org.opencontainers.image.licenses=FSL-1.1-ALv2 \
      org.opencontainers.image.description="Cubepals' node daemon"
ENTRYPOINT ["/blocklyd"]
