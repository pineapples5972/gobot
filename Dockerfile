# Stage 1: Build the Go binary (Upgraded to Go 1.26)
FROM golang:1.26-alpine AS builder

# Install git and certificates (needed for downloading modules and HTTPS scraping)
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy the source code first
COPY . .

# Upgrade the go.mod file to 1.26, then download dependencies
RUN go mod edit -go=1.26
# Force Go to bypass proxy servers and pull directly from GitHub
ENV GOPROXY=https://goproxy.io,direct
RUN go mod tidy

# Build a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /libgen-bot main.go

# Stage 2: Final lightweight image
FROM alpine:3.20

# Install certificates so the bot can securely talk to Telegram and Libgen
RUN apk add --no-cache ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /libgen-bot .

# Run the bot
CMD ["./libgen-bot"]
