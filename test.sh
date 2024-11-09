#!/bin/bash

# Exit on any error
set -e

# Build the Go application
echo "Building Go application..."
tmpFile=$(mktemp)
go build -o "$tmpFile" app/*.go

# Start the Go application in background
echo "Starting Go application on port 6379..."
"$tmpFile" --port 6379 &
GO_PID=$!

# Start Redis server in background
echo "Starting Redis server on port 6380..."
redis-server --port 6380 &
REDIS_PID=$!

# Wait a moment for both servers to start
sleep 2

# Test the Go application with redis-cli
echo "Testing connection to Go application..."
redis-cli -p 6379 set key value
result=$(redis-cli -p 6379 get key)
echo "Response from Go application: $result"

# Clean up
echo "Cleaning up..."
kill $GO_PID
kill $REDIS_PID
rm "$tmpFile"
