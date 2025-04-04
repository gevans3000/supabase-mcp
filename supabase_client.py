"""
Supabase client wrapper for the MCP server.
Provides methods for interacting with a Supabase database.
"""
import os
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()


class SupabaseClient:
    """
    Client for interacting with a Supabase database.
    Handles authentication and provides methods for CRUD operations.
    """

    def __init__(self) -> None:
        """
        Initialize the Supabase client with credentials from environment variables.
        
        Raises:
            ValueError: If required environment variables are missing.
        """
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        
        if not self.url or not self.key:
            raise ValueError(
                "Missing required environment variables: "
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set"
            )
        
        self.client = create_client(self.url, self.key)
    
    def read_records(
        self, 
        table: str, 
        columns: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Read records from a table.
        
        Args:
            table: The name of the table to read from.
            columns: The columns to select, default is "*" for all columns.
            filters: Dictionary of column-value pairs for filtering records.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Dictionary with column name as key and direction as value ("asc" or "desc").
            
        Returns:
            List of dictionaries representing the records.
        """
        query = self.client.table(table).select(columns)
        
        if filters:
            for column, value in filters.items():
                query = query.eq(column, value)
        
        if order_by:
            for column, direction in order_by.items():
                if direction.lower() == "asc":
                    query = query.order(column)
                else:
                    query = query.order(column, desc=True)
        
        if limit:
            query = query.limit(limit)
        
        if offset:
            query = query.offset(offset)
        
        response = query.execute()
        return response.data
    
    def create_records(
        self, 
        table: str, 
        records: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        Create one or more records in a table.
        
        Args:
            table: The name of the table to create records in.
            records: A dictionary or list of dictionaries representing the records to create.
            
        Returns:
            List of dictionaries representing the created records.
        """
        response = self.client.table(table).insert(records).execute()
        return response.data
    
    def update_records(
        self, 
        table: str, 
        updates: Dict[str, Any], 
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Update records in a table that match the given filters.
        
        Args:
            table: The name of the table to update records in.
            updates: Dictionary of column-value pairs to update.
            filters: Dictionary of column-value pairs for filtering records to update.
            
        Returns:
            List of dictionaries representing the updated records.
        """
        query = self.client.table(table)
        
        for column, value in filters.items():
            query = query.eq(column, value)
        
        response = query.update(updates).execute()
        return response.data
    
    def delete_records(
        self, 
        table: str, 
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Delete records from a table that match the given filters.
        
        Args:
            table: The name of the table to delete records from.
            filters: Dictionary of column-value pairs for filtering records to delete.
            
        Returns:
            List of dictionaries representing the deleted records.
        """
        query = self.client.table(table)
        
        for column, value in filters.items():
            query = query.eq(column, value)
        
        response = query.delete().execute()
        return response.data
