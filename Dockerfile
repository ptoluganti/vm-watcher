FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /vm-audit-sink .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /vm-audit-sink /vm-audit-sink
USER nonroot
ENTRYPOINT ["/vm-audit-sink"]
