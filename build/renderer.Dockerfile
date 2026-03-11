# Stage 1: Tailwind CSS Build
FROM debian:bookworm-slim AS tailwind
RUN apt-get update && apt-get install -y curl ca-certificates
RUN curl -fsSLo /usr/local/bin/tailwindcss https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-linux-x64 && \
    chmod +x /usr/local/bin/tailwindcss

WORKDIR /src
# Copy all files that might contain Tailwind classes for scanning
COPY cmd/renderer/templates ./cmd/renderer/templates
COPY cmd/renderer/assets/src ./cmd/renderer/assets/src
RUN mkdir -p ./cmd/renderer/assets/dist

# Build the CSS
RUN tailwindcss -i ./cmd/renderer/assets/src/input.css -o ./cmd/renderer/assets/dist/output.css --minify

# Stage 2: Go Build
FROM golang:1.23@sha256:60deed95d3888cc5e4d9ff8a10c54e5edc008c6ae3fba6187be6fb592e19e8c0 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Copy the generated CSS from the tailwind stage
COPY --from=tailwind /src/cmd/renderer/assets/dist/output.css ./cmd/renderer/assets/dist/output.css
RUN CGO_ENABLED=0 go build -o /out/renderer ./cmd/renderer

# Stage 3: Final Image
FROM gcr.io/distroless/static-debian12@sha256:20bc6c0bc4d625a22a8fde3e55f6515709b32055ef8fb9cfbddaa06d1760f838
COPY --from=build /out/renderer /renderer
ENTRYPOINT ["/renderer"]
