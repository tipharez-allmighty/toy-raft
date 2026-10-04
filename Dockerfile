FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o raft-node .

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/raft-node .

EXPOSE 50051

ENTRYPOINT ["./raft-node"]
