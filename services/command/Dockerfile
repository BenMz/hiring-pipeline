# Build from the repository root: docker build -f services/command/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen/ gen/
COPY services/command/ services/command/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pipeline-command ./services/command/cmd/pipeline-command

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pipeline-command /pipeline-command
EXPOSE 50051
ENTRYPOINT ["/pipeline-command"]
