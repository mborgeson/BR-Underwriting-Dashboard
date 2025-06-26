# Microsoft 365 MCP Server Setup for B&R Capital

## Overview
The Microsoft 365 MCP Server has been configured to integrate with your B&R Capital SharePoint environment, providing direct access to SharePoint files, sites, and data through Claude Code.

## Configuration Files

### `.mcp.json` - Project MCP Configuration
Located in the project root, this file configures the Microsoft 365 MCP server with your B&R Capital Azure AD credentials.

### `.env.mcp` - Environment Variables
Contains your Azure AD configuration (for reference/backup).

## Authentication

The MCP server uses your existing Azure AD app credentials:
- **Client ID**: `5a620cea-31fe-40f6-8b48-d55bc5465dc9` (ExcelGraphAPI)
- **Tenant ID**: `383e5745-a469-4712-aaa9-f7d79c981e10`

## Available Capabilities

Once configured, you'll have access to:

### SharePoint Integration
- **Search SharePoint sites**: Find specific sites and content
- **Access site drives**: List and browse document libraries  
- **File operations**: Read, list, and manage SharePoint files
- **Site management**: Get site details and permissions

### Excel File Access
- **Direct file access**: Read Excel files without downloading
- **Real-time data**: Access current file versions
- **Metadata**: Get file properties and modification dates

## Usage in Claude Code

1. **Start a new session** in the project directory:
   ```bash
   cd "/home/mattb/B&R Programming (WSL)/b_and_r_capital_dashboard"
   claude
   ```

2. **Check MCP server status**:
   ```
   /mcp
   ```

3. **Authenticate with Microsoft 365**:
   The server will prompt you for authentication on first use. You'll need to:
   - Visit the provided URL
   - Enter the device code
   - Sign in with your B&R Capital Microsoft 365 account

4. **Example commands** you can use:
   ```
   # Search for Excel files in SharePoint
   "Find all UW Model files in our SharePoint"
   
   # Access specific site
   "Show me the files in the B&R Capital Real Estate library"
   
   # Get file details
   "Get the metadata for the Emparrado UW Model file"
   ```

## Integration with Existing Workflow

This MCP server complements your existing Excel extraction system:

- **Current system**: Downloads files and extracts data locally
- **MCP server**: Provides direct SharePoint access and metadata
- **Combined benefit**: Real-time file discovery + robust data extraction

## Troubleshooting

### Common Issues:

1. **Authentication errors**: Ensure you're signed into the correct Microsoft 365 tenant
2. **Permission errors**: Verify the Azure AD app has necessary SharePoint permissions
3. **Connection timeouts**: Check network connectivity and firewall settings

### Debug Mode:
Start Claude Code with MCP debugging:
```bash
claude --mcp-debug
```

## Security Notes

- Environment variables contain sensitive Azure AD credentials
- The `.env.mcp` file should not be committed to version control
- Authentication tokens are managed securely by the MCP server

## Next Steps

1. Test the MCP server connection
2. Authenticate with your B&R Capital account  
3. Explore SharePoint integration capabilities
4. Integrate with your existing data extraction workflows