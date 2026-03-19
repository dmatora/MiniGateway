FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/minimax-gateway ./main.go

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=build /out/minimax-gateway /usr/local/bin/minimax-gateway

ENV PORT=3000
ENV DATABASE_PATH=/data/gateway.db
ENV UPSTREAM_BASE_URL=https://api.minimax.io/v1
ENV UPSTREAM_MODELS=MiniMax-M2.7,MiniMax-M2.5,MiniMax-M2.1,MiniMax-M2

VOLUME ["/data"]
EXPOSE 3000

CMD ["minimax-gateway"]
