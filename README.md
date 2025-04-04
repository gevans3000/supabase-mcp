# Supabase MCP Server

A Model Context Protocol (MCP) server for interacting with Supabase databases. This server implements the [Model Context Protocol](https://modelcontextprotocol.ai/) using the [FastMCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) to provide database operation tools for large language models (LLMs).

## Features

- Read records from Supabase tables with filtering, pagination, and sorting
- Create new records (single or batch) in Supabase tables
- Update existing records based on filter conditions
- Delete records from tables based on filter conditions
- Communicates using MCP's Stdio transport

## Prerequisites

- Python 3.8 or higher
- A Supabase project with tables already set up
- Supabase service role key for authentication

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/supabase-mcp.git
   cd supabase-mcp
   ```

2. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   - Copy the `.env.example` file to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Fill in your Supabase URL and service role key in the `.env` file:
     ```
     SUPABASE_URL=your_supabase_project_url
     SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
     ```

## Usage

### Starting the Server

Run the server with the following command:

```bash
python server.py
```

The server uses Stdio transport, so it will listen for MCP requests on standard input and respond on standard output.

### Integrating with MCP Clients

This server implements the Model Context Protocol and can be integrated with any MCP-compatible client. For example, you can use it with LLM frameworks that support MCP tools.

### Tool Descriptions

The server provides the following tools:

#### 1. read_records

Reads records from a Supabase database table with flexible querying options.

**Parameters:**
- `table` (string, required): Name of the table to read from
- `columns` (string, optional, default: "*"): Columns to select (comma-separated or * for all)
- `filters` (object, optional): Filtering conditions as key-value pairs
- `limit` (integer, optional): Maximum number of records to return
- `offset` (integer, optional): Number of records to skip for pagination
- `order_by` (object, optional): Sorting options as column:direction pairs

**Example:**
```json
{
  "table": "users",
  "columns": "id,name,email",
  "filters": {"is_active": true},
  "limit": 10,
  "offset": 0,
  "order_by": {"created_at": "desc"}
}
```

#### 2. create_records

Creates one or more records in a Supabase database table.

**Parameters:**
- `table` (string, required): Name of the table to create records in
- `records` (object or array, required): A single record object or array of record objects to create

**Example (single record):**
```json
{
  "table": "users",
  "records": {
    "name": "John Doe",
    "email": "john@example.com",
    "role": "user"
  }
}
```

**Example (multiple records):**
```json
{
  "table": "users",
  "records": [
    {
      "name": "John Doe",
      "email": "john@example.com",
      "role": "user"
    },
    {
      "name": "Jane Smith",
      "email": "jane@example.com",
      "role": "admin"
    }
  ]
}
```

#### 3. update_records

Updates existing records in a Supabase database table based on filter conditions.

**Parameters:**
- `table` (string, required): Name of the table to update records in
- `updates` (object, required): Fields to update as key-value pairs
- `filters` (object, required): Filtering conditions to identify records to update

**Example:**
```json
{
  "table": "users",
  "updates": {
    "is_verified": true,
    "last_login_at": "2025-04-04T15:30:00Z"
  },
  "filters": {
    "id": 123
  }
}
```

#### 4. delete_records

Deletes records from a Supabase database table based on filter conditions.

**Parameters:**
- `table` (string, required): Name of the table to delete records from
- `filters` (object, required): Filtering conditions to identify records to delete

**Example:**
```json
{
  "table": "expired_sessions",
  "filters": {
    "expires_at": {"lt": "2025-01-01T00:00:00Z"}
  }
}
```

## Development

### Project Structure

- `server.py`: Main MCP server implementation
- `supabase_client.py`: Supabase client wrapper
- `requirements.txt`: Python dependencies
- `.env.example`: Example environment variables file

### Adding New Tools

To add a new tool to the server:

1. Define a Pydantic model for the tool's request parameters in `server.py`
2. Add a handler method to the `SupabaseMCPServer` class
3. Register the tool in the `_register_tools` method with a descriptive name and documentation

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
