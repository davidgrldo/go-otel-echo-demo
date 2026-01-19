# build stage
FROM golang:1.23.4-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/server

# runtime stage
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /app/server /server

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/server"]
