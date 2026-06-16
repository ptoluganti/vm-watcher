# Build stage
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /audit-sink ./cmd/audit-sink

# Runtime stage — minimal distroless image, runs as nonroot (uid 65532)
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /audit-sink /audit-sink
ENTRYPOINT ["/audit-sink"]
