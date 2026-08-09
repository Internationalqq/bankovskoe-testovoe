FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /bin/bankovskoe ./cmd/app

FROM alpine:3.22

RUN adduser -D -H appuser

WORKDIR /app
COPY --from=builder /bin/bankovskoe /app/bankovskoe

EXPOSE 8090

USER appuser
ENTRYPOINT ["/app/bankovskoe"]
