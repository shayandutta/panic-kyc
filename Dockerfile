# syntax=docker/dockerfile:1
# One Dockerfile for every service: pass the service's main package as
# SERVICE_PATH, e.g. --build-arg SERVICE_PATH=services/api-gateway/cmd

# Build stage: full Go toolchain
FROM golang:1.23-alpine AS build
WORKDIR /src

# Cache mounts keep downloaded modules and Go's compile cache between
# builds, so a rebuild only recompiles packages that actually changed.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG SERVICE_PATH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./${SERVICE_PATH}

# Run stage: no shell, no package manager, runs as a non-root user.
# The final image is the binary plus a few MB.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/app /app
COPY deploy /deploy
USER nonroot:nonroot
ENTRYPOINT ["/app"]
