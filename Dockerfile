FROM golang:latest

WORKDIR /app

ARG CONF="./config/config.yaml.example"

COPY ${CONF} ./config/config.yaml
COPY . .

ENV REDIS_URL="localhost:6379"

RUN go mod download
RUN mkdir /app/bin
RUN go build -o /app/bin/eth-tx-crawler ./cmd

ENTRYPOINT ["/app/bin/eth-tx-crawler"]
