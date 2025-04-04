# Supabase MCP Server - Project Planning

## Project Overview
This project implements a Model Context Protocol (MCP) server that provides tools for interacting with a Supabase database. The server enables AI assistants to perform database operations through a standardized interface.

## Architecture
- **Transport Layer**: Stdio transport for communication
- **Protocol**: Model Context Protocol (MCP)
- **Framework**: FastMCP Python SDK
- **Database**: Supabase (PostgreSQL)

## Components
1. **MCP Server**
   - Implements the Model Context Protocol
   - Uses FastMCP for server implementation
   - Communicates via Stdio transport

2. **Supabase Client**
   - Handles authentication with Supabase
   - Performs database operations

3. **MCP Tools**
   - Read records from tables
   - Create records in tables
   - Update records in tables
   - Delete records from tables

## Environment Configuration
- `SUPABASE_URL`: URL of the Supabase project
- `SUPABASE_SERVICE_ROLE_KEY`: Service role key for Supabase authentication

## File Structure
```
supabase-mcp/
├── server.py              # Main MCP server implementation
├── supabase_client.py     # Supabase client wrapper
├── requirements.txt       # Python dependencies
├── .env.example           # Example environment variables
├── README.md              # Project documentation
├── PLANNING.md            # Project planning (this file)
└── TASK.md                # Task tracking
```

## Style Guidelines
- Follow PEP8 standards
- Use type hints for all functions
- Document functions with Google-style docstrings
- Format code with Black
- Use Pydantic for data validation

## Dependencies
- mcp
- supabase
- python-dotenv