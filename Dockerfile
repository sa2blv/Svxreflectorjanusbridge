FROM golang:1.22-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus-dev libopusfile-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the module definitions over
COPY go.mod go.sum* ./
RUN go mod download

# Copy the rest of your source code scripts over
COPY *.go ./

# FORCE GO TO AUTOMATICALLY RESOLVE MISSING CHECKSUMS INSIDE CONTAINER
RUN go mod tidy

# Compile the binary securely with CGO enabled for libopus
RUN CGO_ENABLED=1 go build -o svx_opus_bridge .

# --- Runtime stage ---
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 libopusfile0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/svx_opus_bridge /usr/local/bin/svx_opus_bridge

ENTRYPOINT ["svx_opus_bridge"]