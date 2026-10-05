FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o raft-node ./cmd/node
RUN CGO_ENABLED=0 GOOS=linux go build -o raft-cli ./cmd/cli

FROM alpine:latest
WORKDIR /app

copy --from=builder /app/raft-node .
copy --from=builder /app/raft-cli .

EXPOSE 50051
