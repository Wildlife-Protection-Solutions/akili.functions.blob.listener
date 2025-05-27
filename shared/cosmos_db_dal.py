# src/dal/cosmos_db_dal.py
# A base class for interacting with Cosmos DB.

from typing import Optional
from azure.cosmos import CosmosClient, exceptions
import settings

class BaseCosmosDBDAL:
    def __init__(self, container_name: str):
        """ Initializes the CosmosDBDAL with the Cosmos DB account settings. """
        self.client = CosmosClient(settings.COSMOS_DB_ENDPOINT, settings.COSMOS_DB_KEY)
        self.database = self.client.get_database_client(settings.COSMOS_DB_DATABASE)
        self.container = self.database.get_container_client(container_name)

    def add_item(self, item: dict):
        """
        Adds a new item to the Cosmos DB container.

        Args:
            item (dict): the item to add
        
        Returns:
            CosmosDict: the created item or None if the operation fails
        """
        try:
            created_item = self.container.create_item(body=item)
            print(f"Item added successfully: {created_item}")
            return created_item
        except exceptions.CosmosHttpResponseError as e:
            print(f"Error adding item: {e}")
            return None

    def get_item(self, item_id: str, partition_key: str | int):
        """ Get an item from the Cosmos DB container.

        Args:
            item_id (str): the id of the item to get
            partition_key (str | int): the partition key of the item

        Returns:
            CosmosDict: the item if found, None otherwise
        """
        try:
            item = self.container.read_item(item_id, partition_key=partition_key)
            return item
        except exceptions.CosmosResourceNotFoundError:
            print(f"Item not found: {item_id}")
            return None

    def update_item(self, item_id: str, item: dict):
        """ Update an item in the Cosmos DB container.

        Args:
            item_id (str): the id of the item to update
            item (dict): the updated item

        Returns:
            CosmosDict:  the updated item if successful, None otherwise
        """
        try:
            updated_item = self.container.replace_item(item_id, body=item)
            print(f"Item updated successfully: {updated_item}")
            return updated_item
        except exceptions.CosmosHttpResponseError as e:
            print(f"Error updating item: {e}")
            return None

    def delete_item(self, item_id: str, partition_key: str | int):
        """ Delete an item from the Cosmos DB container.

        Args:
            item_id (str): the id of the item to delete
            partition_key (str | int): the partition key of the item
        """
        try:
            self.container.delete_item(item_id, partition_key=partition_key)
            print(f"Item deleted successfully: {item_id}")
        except exceptions.CosmosHttpResponseError as e:
            print(f"Error deleting item: {e}")
            
    def execute_batch_items(self, batch_operations: list, partition_key: str | int):
        """ Execute multiple operations to the Cosmos DB container.

        Args:
            batch_operations (list): the list of operations to execute
                Each operation is a tuple of (operation_type, item, options)
                where operation_type is one of 'create', 'replace', 'delete', etc.
                item is the item to operate on and options are additional options.
            partition_key (str | int): the partition key for the operations
            
        Returns:
            list: the results of the batch operations or None if  one of the operations fails
        """
        try:
            batch_results = self.container.execute_item_batch(batch_operations=batch_operations, partition_key=partition_key)
            return batch_results
        except exceptions.CosmosBatchOperationError as e:
            error_operation_index = e.error_index
            error_operation_response = e.operation_responses[error_operation_index]
            error_operation = batch_operations[error_operation_index]
            print("\nError operation: {}, error operation response: {}\n".format(error_operation, error_operation_response))
            return None
        
    def query_item_by_partition(
        self,
        partition_key_value: str | int,
        field_name="*",
        skip: int=0,
        take: int=None,
        additional_where: Optional[str]=None
    ):
        """
        Query for specific fields from documents in a partition with pagination.
        
        Args:
            partition_key_value: Value of the partition key to query
            field_name: Field to retrieve (use "*" for all fields, "field1, field2" for multiple fields)
            skip: Number of items to skip (for pagination)
            take: Maximum number of items to return (None for all)
            additional_where: Additional WHERE clause conditions (without the "WHERE" keyword)
        
        Returns:
            List of query results
        """
        # Handle field projection
        if field_name == "*":
            select_clause = "*"
        elif "," in field_name:
            # Multiple fields requested
            select_clause = f"c.{field_name.replace(',', ', c.')}"
        else:
            # Single field requested
            select_clause = f"c.{field_name}"
        
        # Build the query
        query = f"SELECT {select_clause} FROM c"  
        # Add WHERE clause if provided
        if additional_where:
            query += f" WHERE {additional_where}"
        # Support pagination
        if skip > 0 or take is not None:
            query += f" OFFSET {skip} LIMIT {take}"

        items = list(self.container.query_items(
            query=query,
            partition_key=partition_key_value
        ))
        return items
