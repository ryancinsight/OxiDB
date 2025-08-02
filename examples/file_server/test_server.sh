#!/bin/bash

echo "Starting file server test..."

# Kill any existing server
pkill -f file_server || true

# Start the server in the background
echo "Starting server..."
cargo run --bin file_server --quiet 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 5

# Check if server is running
if ! ps -p $SERVER_PID > /dev/null; then
    echo "Server failed to start"
    exit 1
fi

echo "Server started with PID: $SERVER_PID"

# Test the health endpoint
echo "Testing health endpoint..."
curl -s http://localhost:3000/api/health && echo " - Health check passed" || echo " - Health check failed"

# Test the register endpoint
echo "Testing register endpoint..."
curl -s -X POST http://localhost:3000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","email":"test@example.com","password":"password123"}' \
  | jq . || echo "Registration test completed"

# Test the login endpoint
echo "Testing login endpoint..."
curl -s -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","password":"password123"}' \
  | jq . || echo "Login test completed"

# Kill the server
echo "Stopping server..."
kill $SERVER_PID

echo "Test completed!"