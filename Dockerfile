# Dockerfile de produção do WaCalls (liongo-calls) — RECUPERADO em 2026-06-30
# a partir do build record do buildx (build iwviniolzazybq0i3lbxm5nme, rev 4e71bef).
# O original ficava em /tmp/WaCalls/Dockerfile e se perdeu quando o clone temporário
# foi limpo. Multi-stage: codec opus_mlow (CGO) + client React + runtime debian enxuto.

# --- Stage native: builda o codec opus_mlow estático (libopus.a) ---
FROM debian:bookworm-slim AS native
RUN apt-get update && apt-get install -y --no-install-recommends \
      git cmake ninja-build build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 https://github.com/edgardmessias/opus_mlow.git
WORKDIR /src/opus_mlow
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DOPUS_BUILD_SHARED_LIBRARY=OFF \
    && cmake --build build

# --- Stage server: compila o binário Go com CGO + tag mlow ---
FROM golang:1.26-bookworm AS server
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=native /src/opus_mlow/build/libopus.a ./native/libopus_mlow.a
RUN CGO_ENABLED=1 go build -tags mlow -o /wacalls-server ./cmd/server

# --- Stage client: builda o front React ---
FROM node:22-slim AS client
WORKDIR /app/client
COPY client/package*.json ./
RUN npm install
COPY client/ ./
RUN npm run build

# --- Stage runtime: debian enxuto só com o binário + dist ---
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data
WORKDIR /app
COPY --from=server /wacalls-server /app/wacalls-server
COPY --from=client /app/client/dist /app/client/dist
EXPOSE 8080
ENTRYPOINT ["/app/wacalls-server", "-addr", ":8080", "-static", "/app/client/dist", "-db", "/data/wacalls.db"]
