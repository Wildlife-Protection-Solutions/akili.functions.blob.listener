import azure.functions as func
import json
import logging
from datetime import datetime
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient
import os
from typing import Dict, Any, Optional

# Configuration
COSMOS_ENDPOINT = os.environ["COSMOS_ENDPOINT"]
COSMOS_KEY = os.environ["COSMOS_KEY"]
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "BlobMonitor")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "BlobMetadata")
CONFIG_CONTAINER_NAME = os.environ.get("CONFIG_CONTAINER_NAME", "Configuration")

# Initialize Cosmos client
cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
metadata_container = database.get_container_client(COSMOS_CONTAINER_NAME)
config_container = database.get_container_client(CONFIG_CONTAINER_NAME)

app = func.FunctionApp()

def get_storage_account_config() -> Dict[str, Any]:
    """Retrieve active storage account configurations from Cosmos DB."""
    try:
        # Query for active configurations
        query = "SELECT * FROM c WHERE c.enabled = true"
        configs = list(config_container.query_items(query=query, enable_cross_partition_query=True))
        
        # Convert to dictionary for easier lookup
        config_dict = {}
        for config in configs:
            account_name = config.get('storage_account_name')
            if account_name:
                config_dict[account_name] = {
                    'connection_string': config.get('connection_string'),
                    'containers': config.get('containers', []),
                    'enabled': config.get('enabled', False)
                }
        
        return config_dict
    except exceptions.CosmosResourceNotFoundError:
        logging.warning("Configuration container not found")
        return {}
    except Exception as e:
        logging.error(f"Error retrieving configuration: {str(e)}")
        return {}

def should_process_blob(storage_account_name: str, container_name: str) -> bool:
    """Check if we should process blobs from this storage account and container."""
    config = get_storage_account_config()
    
    if storage_account_name not in config:
        return False
    
    account_config = config[storage_account_name]
    if not account_config.get('enabled', False):
        return False
    
    # If containers list is empty, monitor all containers in the account
    containers = account_config.get('containers', [])
    if not containers:
        return True
    
    # Check if this specific container is in the monitored list
    return container_name in containers

def extract_blob_metadata(blob_url: str, storage_account_name: str) -> Optional[Dict[str, Any]]:
    """Extract metadata from blob using the configured connection string."""
    try:
        config = get_storage_account_config()
        if storage_account_name not in config:
            logging.error(f"No configuration found for storage account: {storage_account_name}")
            return None
        
        connection_string = config[storage_account_name].get('connection_string')
        if not connection_string:
            logging.error(f"No connection string found for storage account: {storage_account_name}")
            return None
        
        # Parse blob URL to get container and blob name
        url_parts = blob_url.split('/')
        container_name = url_parts[-2]
        blob_name = url_parts[-1]
        
        # Get blob client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Get blob properties
        properties = blob_client.get_blob_properties()
        
        metadata = {
            'id': f"{storage_account_name}_{container_name}_{blob_name}_{datetime.utcnow().isoformat()}",
            'storage_account_name': storage_account_name,
            'container_name': container_name,
            'blob_name': blob_name,
            'blob_url': blob_url,
            'size': properties.size,
            'content_type': properties.content_settings.content_type,
            'etag': properties.etag,
            'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
            'creation_time': properties.creation_time.isoformat() if properties.creation_time else None,
            'content_md5': properties.content_settings.content_md5,
            'metadata': dict(properties.metadata) if properties.metadata else {},
            'processed_at': datetime.utcnow().isoformat(),
            'partition_key': f"{storage_account_name}_{container_name}"
        }
        
        return metadata
        
    except Exception as e:
        logging.error(f"Error extracting blob metadata: {str(e)}")
        return None

def save_metadata_to_cosmos(metadata: Dict[str, Any]) -> bool:
    """Save blob metadata to Cosmos DB."""
    try:
        metadata_container.create_item(body=metadata)
        logging.info(f"Successfully saved metadata for blob: {metadata['blob_name']}")
        return True
    except exceptions.CosmosResourceExistsError:
        logging.warning(f"Metadata already exists for blob: {metadata['blob_name']}")
        return True
    except Exception as e:
        logging.error(f"Error saving metadata to Cosmos DB: {str(e)}")
        return False

@app.event_grid_trigger(arg_name="azeventgrid")
def blob_created_handler(azeventgrid: func.EventGridEvent):
    """Handle blob created events from Event Grid."""
    try:
        # Parse event data
        event_data = azeventgrid.get_json()
        logging.info(f"Received event: {json.dumps(event_data, indent=2)}")
        
        # Extract information from event
        blob_url = event_data.get('url', '')
        event_type = azeventgrid.event_type
        
        # Only process blob creation events
        if event_type != 'Microsoft.Storage.BlobCreated':
            logging.info(f"Ignoring event type: {event_type}")
            return
        
        # Extract storage account name from URL
        storage_account_name = blob_url.split('//')[1].split('.')[0]
        container_name = blob_url.split('/')[-2]
        
        logging.info(f"Processing blob from storage account: {storage_account_name}, container: {container_name}")
        
        # Check if we should process this blob
        if not should_process_blob(storage_account_name, container_name):
            logging.info(f"Skipping blob processing for {storage_account_name}/{container_name} - not configured or disabled")
            return
        
        # Extract blob metadata
        metadata = extract_blob_metadata(blob_url, storage_account_name)
        if not metadata:
            logging.error("Failed to extract blob metadata")
            return
        
        # Save to Cosmos DB
        if save_metadata_to_cosmos(metadata):
            logging.info(f"Successfully processed blob: {metadata['blob_name']}")
        else:
            logging.error(f"Failed to save metadata for blob: {metadata['blob_name']}")
            
    except Exception as e:
        logging.error(f"Error processing blob event: {str(e)}")
        raise