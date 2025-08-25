# syntax=docker/dockerfile:1
FROM golang:1.24 AS builder

WORKDIR /build
ADD . /build/

# Create output directory
RUN mkdir /out

# Generate code (templ and sqlc)
RUN go generate ./...

# Build all binaries with CGO enabled and static linking
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod/ \
    CGO_ENABLED=1 go build -ldflags="-w -s -linkmode external -extldflags '-static'" -o /out/datas3t ./cmd/datas3t

# Create final image
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates curl

WORKDIR /app

# Copy SSL certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy all binaries
COPY --from=builder /out/datas3t /app/

# Set default entrypoint to server
ENTRYPOINT ["/app/datas3t","server"]