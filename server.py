"""
Supabase MCP Server

A FastMCP server that provides tools for interacting with a Supabase database.
Uses the Model Context Protocol (MCP) with Stdio transport.
"""

from typing import Any, Dict, List, Optional, Union
import json
import sys

from fastmcp import FastMCP, StdioTransport
from pydantic import BaseModel, Field

from supabase_client import SupabaseClient


# ----- Request/Response Models -----

class ReadRecordsRequest(BaseModel):
    table: str = Field(..., description="Name of the table to read from")
    columns: str = Field("*", description="Columns to select (comma-separated or * for all)")
    filters: Optional[Dict[str, Any]] = Field(
        None, 
        description="Filtering conditions as key-value pairs (e.g., {\"column\": \"value\"} for column = value)"
    )
    limit: Optional[int] = Field(None, description="Maximum number of records to return")
    offset: Optional[int] = Field(None, description="Number of records to skip for pagination")
    order_by: Optional[Dict[str, str]] = Field(
        None, 
        description="Sorting options as column:direction pairs (e.g., {\"created_at\": \"desc\"})"
    )


class CreateRecordsRequest(BaseModel):
    table: str = Field(..., description="Name of the table to create records in")
    records: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(
        ..., 
        description="A single record object or array of record objects to create"
    )


class UpdateRecordsRequest(BaseModel):
    table: str = Field(..., description="Name of the table to update records in")
    updates: Dict[str, Any] = Field(..., description="Fields to update as key-value pairs")
    filters: Dict[str, Any] = Field(
        ..., 
        description="Filtering conditions to identify records to update (e.g., {\"id\": 123})"
    )


class DeleteRecordsRequest(BaseModel):
    table: str = Field(..., description="Name of the table to delete records from")
    filters: Dict[str, Any] = Field(
        ..., 
        description="Filtering conditions to identify records to delete (e.g., {\"id\": 123})"
    )


# ----- MCP Server Class -----

class SupabaseMCPServer:
    """
    Model Context Protocol (MCP) server for Supabase database operations.
    Provides tools for reading, creating, updating, and deleting records in Supabase tables.
    """

    def __init__(self):
        """Initialize the Supabase MCP server with a Supabase client."""
        self.supabase = SupabaseClient()
        self.mcp = FastMCP(
            transport=StdioTransport(),
            server_name="supabase-mcp",
            server_version="0.1.0",
        )
        self._register_tools()

    def _register_tools(self):
        """Register all MCP tools with detailed descriptions."""
        self.mcp.register_tool(
            name="read_records",
            description=(
                "Reads records from a Supabase database table with flexible querying options. "
                "Use this tool to fetch data from your database with options for filtering, "
                "sorting, and pagination. "
                "\n\n"
                "Common use cases:\n"
                "- Retrieve all records from a table\n"
                "- Fetch specific records based on conditions\n"
                "- Get paginated results for large datasets\n"
                "- Retrieve only specific columns from records\n"
                "\n"
                "The returned data is always an array of record objects, even if only one record is found. "
                "If no records match the criteria, an empty array is returned."
            ),
            handler=self.read_records,
            request_model=ReadRecordsRequest,
        )

        self.mcp.register_tool(
            name="create_records",
            description=(
                "Creates one or more records in a Supabase database table. "
                "Use this tool to insert new data into your database. "
                "\n\n"
                "Common use cases:\n"
                "- Add a single new record to a table\n"
                "- Bulk insert multiple records at once\n"
                "- Create related records\n"
                "\n"
                "You can provide either a single record object or an array of record objects. "
                "The tool returns the created records with their assigned IDs and timestamps (if applicable). "
                "Make sure the data structure matches the table schema to avoid validation errors."
            ),
            handler=self.create_records,
            request_model=CreateRecordsRequest,
        )

        self.mcp.register_tool(
            name="update_records",
            description=(
                "Updates existing records in a Supabase database table based on filter conditions. "
                "Use this tool to modify existing data that matches specific criteria. "
                "\n\n"
                "Common use cases:\n"
                "- Update a specific record by ID\n"
                "- Batch update multiple records matching a condition\n"
                "- Modify specific fields while keeping others unchanged\n"
                "\n"
                "The 'updates' parameter specifies which fields to update and their new values. "
                "The 'filters' parameter determines which records will be updated. "
                "Be careful with filter conditions - if they match many records, all of them will be updated. "
                "The tool returns the updated records after modification."
            ),
            handler=self.update_records,
            request_model=UpdateRecordsRequest,
        )

        self.mcp.register_tool(
            name="delete_records",
            description=(
                "Deletes records from a Supabase database table based on filter conditions. "
                "Use this tool to remove data that matches specific criteria. "
                "\n\n"
                "Common use cases:\n"
                "- Delete a specific record by ID\n"
                "- Remove multiple records matching a condition\n"
                "- Clean up old or unnecessary data\n"
                "\n"
                "⚠️ IMPORTANT: Deletions are permanent and cannot be undone. Always confirm the filter "
                "conditions carefully before deleting records. "
                "For safety, always use specific filter conditions to avoid accidentally deleting too many records. "
                "The tool returns the deleted records as they were before deletion."
            ),
            handler=self.delete_records,
            request_model=DeleteRecordsRequest,
        )

    # ----- Tool Handlers -----

    def read_records(self, request: ReadRecordsRequest) -> Dict[str, Any]:
        """
        Handler for the read_records tool.
        
        Args:
            request: The validated request containing table name and query parameters.
            
        Returns:
            Dictionary with records array.
        """
        try:
            records = self.supabase.read_records(
                table=request.table,
                columns=request.columns,
                filters=request.filters,
                limit=request.limit,
                offset=request.offset,
                order_by=request.order_by
            )
            return {"records": records}
        except Exception as e:
            return {"error": str(e), "records": []}

    def create_records(self, request: CreateRecordsRequest) -> Dict[str, Any]:
        """
        Handler for the create_records tool.
        
        Args:
            request: The validated request containing table name and record data.
            
        Returns:
            Dictionary with created records array.
        """
        try:
            created = self.supabase.create_records(
                table=request.table,
                records=request.records
            )
            return {"records": created}
        except Exception as e:
            return {"error": str(e), "records": []}

    def update_records(self, request: UpdateRecordsRequest) -> Dict[str, Any]:
        """
        Handler for the update_records tool.
        
        Args:
            request: The validated request containing table name, updates, and filters.
            
        Returns:
            Dictionary with updated records array.
        """
        try:
            updated = self.supabase.update_records(
                table=request.table,
                updates=request.updates,
                filters=request.filters
            )
            return {"records": updated}
        except Exception as e:
            return {"error": str(e), "records": []}

    def delete_records(self, request: DeleteRecordsRequest) -> Dict[str, Any]:
        """
        Handler for the delete_records tool.
        
        Args:
            request: The validated request containing table name and filters.
            
        Returns:
            Dictionary with deleted records array.
        """
        try:
            deleted = self.supabase.delete_records(
                table=request.table,
                filters=request.filters
            )
            return {"records": deleted}
        except Exception as e:
            return {"error": str(e), "records": []}

    def start(self):
        """Start the MCP server and begin processing requests."""
        self.mcp.start()


if __name__ == "__main__":
    server = SupabaseMCPServer()
    server.start()
