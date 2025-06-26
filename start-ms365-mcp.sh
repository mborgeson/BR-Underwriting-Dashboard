#!/bin/bash

# B&R Capital Microsoft 365 MCP Server Wrapper
# This script sets up the environment and starts the MCP server

# Set environment variables for B&R Capital Azure AD
export MS365_MCP_CLIENT_ID="5a620cea-31fe-40f6-8b48-d55bc5465dc9"
export MS365_MCP_TENANT_ID="383e5745-a469-4712-aaa9-f7d79c981e10"
export ENABLED_TOOLS="sharepoint.*|search.*|site.*|drive.*"
export READ_ONLY="false"

# Disable keytar to avoid WSL library issues
export ELECTRON_SKIP_BINARY_DOWNLOAD=1
export USE_KEYTAR=false

echo "Starting Microsoft 365 MCP Server for B&R Capital..."
echo "Client ID: ${MS365_MCP_CLIENT_ID}"
echo "Tenant ID: ${MS365_MCP_TENANT_ID}"

# Try to start the server
node /home/mattb/.nvm/versions/node/v18.20.8/lib/node_modules/@softeria/ms-365-mcp-server/dist/index.js "$@"