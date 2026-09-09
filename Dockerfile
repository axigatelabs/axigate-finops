# The gateway is a single static binary with no runtime dependency, so the
# image is a scratch-like distroless base with just the binary and CA roots
# (needed to reach the providers over TLS). Nothing else ships.
FROM golang:1.26 AS build
WORKDIR /src
# The module is stdlib-only, so there is nothing to download; copy and build.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/axigate-finops ./cmd/axigate-finops
# The ledger dir must be writable by the non-root runtime user (uid 65532), so
# create it here and carry that ownership into the image; the anonymous volume
# Docker makes for it then inherits it and the default one-liner can write.
RUN mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/axigate-finops /usr/local/bin/axigate-finops
# The ledger is written here; mount a volume so events survive the container.
COPY --from=build --chown=65532:65532 /data /data
VOLUME ["/data"]
# 8080 is the gateway (point your app's base URL here); 8906 is the dashboard.
EXPOSE 8080 8906
ENTRYPOINT ["/usr/local/bin/axigate-finops"]
# Default: the whole product in one command — the OpenAI gateway on 8080 and the
# live dashboard on 8906, sharing one ledger under the volume. Override the args
# to switch provider (--provider anthropic), set caps, or change the ports.
CMD ["serve", "--provider", "openai", "--gateway-listen", "0.0.0.0:8080", "--console-listen", "0.0.0.0:8906", "--ledger", "/data/gateway-events.jsonl"]
