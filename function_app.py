import datetime
import json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse
import azure.functions as func
from azure.cosmos import CosmosClient, ContainerProxy
import os
from azure.storage.blob import BlobServiceClient

from shared.deployment_cosmos_db_dal import DeploymentCosmosDBDAL

COSMOS_DB_ENDPOINT = os.environ["COSMOS_DB_ENDPOINT"]
COSMOS_DB_KEY = os.environ["COSMOS_DB_KEY"]
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME")
COSMOS_DEPLOYMENTS_CONTAINER = os.environ.get("COSMOS_DEPLOYMENTS_DB_CONTAINER")
COSMOS_CONFIG_CONTAINER = os.environ.get("COSMOS_CONFIG_DB_CONTAINER")

client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
database = client.get_database_client(COSMOS_DATABASE_NAME)
deployment_container: ContainerProxy = database.get_container_client(COSMOS_DEPLOYMENTS_CONTAINER)
# config_container: ContainerProxy = database.get_container_client(COSMOS_CONFIG_CONTAINER)

app = func.FunctionApp()

@app.function_name(name="eventgridtrigger1")
@app.event_grid_trigger(arg_name="event")
def test_function(event: func.EventGridEvent):

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)

    try:
        # Parse event data
        event_data = event.get_json()
        
        # Extract information from event
        blob_url = event_data.get('url', '')
        event_type = event.event_type
        
        # Only process blob creation events
        if event_type != 'Microsoft.Storage.BlobCreated':
            logging.info(f"Ignoring event type: {event_type}")
            return
        
        # Extract storage account name from URL
        storage_account_name = blob_url.split('//')[1].split('.')[0]
        container_name = extract_container_name(blob_url)
        logging.info(f"Processing blob from storage account: {storage_account_name}, container: {container_name}, event: {event_data}")
        
        # Extract blob metadata
        metadata = extract_blob_metadata(
            blob_url=blob_url,
            storage_account_name=storage_account_name
        )
        # If metadata extraction failed, log and return
        if metadata is None:
            logging.error(f"Failed to extract metadata for blob: {blob_url}")
            return
        logging.info(f"Extracted metadata: {json.dumps(metadata, indent=2)}")
        # Initialize DAL
        dal = DeploymentCosmosDBDAL(
            container=deployment_container
        )
        # Add metadata to Cosmos DB
        deployment_id = metadata.get('metadata', {}).get('dep_id')
        filehash = metadata.get('metadata', {}).get('hash')
        try:
            deployment_id = int(deployment_id) if deployment_id else None
        except ValueError:
            logging.error(f"Invalid deployment_id: {deployment_id}. Must be an integer.")
            return
        if not deployment_id or not filehash:
            logging.error(f"Missing deployment_id or filehash in metadata: {metadata.get('metadata', {})}")
            return
        logging.info(f"Adding metadata for deployment_id: {deployment_id}, filehash: {filehash}")
        dal.add_file_hash(
            deployment_id=deployment_id,
            file_hash=filehash,
        )
        logging.info(f"Successfully added metadata")

            
    except Exception as e:
        logging.error(f"Error processing blob event: {str(e)}")
        raise
    
def extract_container_name(blob_url: str) -> str:
    path_parts = urlparse(blob_url).path.strip('/').split('/')
    # Take the first two parts after the domain
    return '/'.join(path_parts[:2])

def extract_blob_metadata(blob_url: str, storage_account_name: str) -> Optional[Dict[str, Any]]:
    """Extract metadata from blob using the configured connection string."""
    try:        
        connection_string = os.environ.get(f"CONNECTION_STRING")
        
        # Parse blob URL to get container and blob name
        url_parts = blob_url.split('/')
        container_name = extract_container_name(blob_url)
        logging.info(f"Margaux - Extracted container name: {container_name} from blob URL: {blob_url}")
        blob_name = url_parts[-1]
        
        # Get blob client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Get blob properties
        properties = blob_client.get_blob_properties()
        
        logging.info(f"margaux - Retrieved properties for blob: {blob_name} in container: {container_name}, metadata: {dict(properties.metadata) if properties.metadata else {}}")
        
        metadata = {
            'storage_account_name': storage_account_name,
            'container_name': container_name,
            'blob_name': blob_name,
            'blob_url': blob_url,
            'size': properties.size,
            'content_type': properties.content_settings.content_type,
            # 'etag': properties.etag,
            # 'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
            # 'creation_time': properties.creation_time.isoformat() if properties.creation_time else None,
            # 'content_md5': properties.content_settings.content_md5.decode('utf-8') if properties.content_settings.content_md5 else None,
            'metadata': dict(properties.metadata) if properties.metadata else {},
        }
        logging.info(f"Extracted metadata: {json.dumps(metadata, indent=2)}")
        
        return metadata
        
    except Exception as e:
        logging.error(f"Error extracting blob metadata: {str(e)}")
        return None
    