# One Dockerfile for every service: pass the service's main package as
# SERVICE_PATH, e.g. --build-arg SERVICE_PATH=services/api-gateway/cmd

# Build stage: full Go toolchain
FROM golang:1.23-alpine AS build
WORKDIR /src

# Download dependencies first so this layer is cached between code changes
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG SERVICE_PATH
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./${SERVICE_PATH}

# Run stage: no shell, no package manager, runs as a non-root user.
# The final image is the binary plus a few MB.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/app /app
COPY deploy /deploy
USER nonroot:nonroot
ENTRYPOINT ["/app"]
