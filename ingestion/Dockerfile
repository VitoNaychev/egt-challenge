# Build from the repo root (shared go.mod):
# docker build -f ingestion/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src

RUN CGO_ENABLED=0 go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.42

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /bin/app ./ingestion/cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /go/bin/grpc-health-probe /grpc_health_probe
COPY --from=build /bin/app /app
ENTRYPOINT ["/app"]
