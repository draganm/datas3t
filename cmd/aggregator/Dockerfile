FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o /aggregator ./cmd/aggregator

# Final stage
FROM alpine:latest

WORKDIR /

# Copy the binary from the builder stage
COPY --from=builder /aggregator /aggregator

# Set environment variables (defaults can be overridden at runtime)
ENV DATAS3T_SERVER_URL="http://datas3t-server:8080"
ENV DATAS3T_AGGREGATION_INTERVAL="30m"
ENV DATAS3T_MIN_DATARANGE_SIZE="104857600"
ENV DATAS3T_IDEAL_DATAPOINT_NUM="1000"

# Run the application
ENTRYPOINT ["/aggregator"] 