# multi-stage build

#
# build stage
#
FROM golang:alpine AS builder

# Add Maintainer Info
LABEL maintainer="ming"

RUN apk --no-cache add build-base git gcc
WORKDIR /root/
ADD . /root
RUN cd /root/src && go build -o pulsar-heartbeat

# Add debug tool
RUN go get github.com/google/gops

#
# Start a new stage from scratch
#
FROM alpine
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /root/src/pulsar-heartbeat /app/

# a kesque cert but it can overwritten by mounting the same path ca-bundle.crt
COPY --from=builder /root/config/kesque-pulsar.cert /etc/ssl/certs/ca-bundle.crt

# Copy debug tools
COPY --from=builder /go/bin/gops /app/gops

ENTRYPOINT ./pulsar-heartbeat ./runtime.yml
