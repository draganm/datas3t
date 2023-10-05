# syntax=docker/dockerfile:1
FROM golang:1.21-alpine as builder
WORKDIR /build
ADD . /build/
RUN mkdir /out
RUN --mount=type=cache,target=/root/.cache/go-build go build -o /out .

FROM alpine
RUN apk add --no-cache
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/* /app
EXPOSE 3322
ENTRYPOINT ["/app/datas3t"]
