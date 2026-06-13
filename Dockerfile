# Stage 1: Build the Go binary (Using Go 1.25)
FROM golang:1.25-alpine AS builder

# Install git and certificates
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy your code into the container
COPY . .

# Clear out any corrupted module data
RUN go clean -modcache

# Force Go to use the official global proxy (this bypasses the broken goproxy.io cache)
ENV GOPROXY=https://proxy.golang.org,direct

# Let Go naturally resolve and download the correct v5 modules
RUN go mod tidy

# Build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /libgen-bot main.go

# Stage 2: Final lightweight image
FROM alpine:3.20

# Install certificates so the bot can securely talk to Telegram and Libgen
RUN apk add --no-cache ca-certificates ghostscript

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /libgen-bot .

# Run the bot
CMD ["./libgen-bot"]
