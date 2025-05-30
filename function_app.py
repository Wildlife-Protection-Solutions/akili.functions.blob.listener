import datetime
import json
import logging
from typing import Any, Dict, Optional
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

@app.function_name(name="get_deployment_metadata")
@app.route(route="config", methods=["GET"])
def get_configurations(req: func.HttpRequest) -> func.HttpResponse:
    """Get all storage account configurations."""
    try:
        test_config = {
            "margaux": "hi",
        }
        return func.HttpResponse(
            json.dumps(test_config),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error retrieving configurations: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to retrieve configurations: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

# @app.route(route="config/{storage_account_name}", methods=["GET"])
# def get_configuration(req: func.HttpRequest) -> func.HttpResponse:
#     """Get configuration for a specific storage account."""
#     try:
#         storage_account_name = req.route_params.get('storage_account_name')
        
#         config = config_container.read_item(
#             item=storage_account_name,
#             partition_key=storage_account_name
#         )
        
#         return func.HttpResponse(
#             json.dumps(config, indent=2),
#             status_code=200,
#             mimetype="application/json"
#         )
        
#     except exceptions.CosmosResourceNotFoundError:
#         return func.HttpResponse(
#             json.dumps({"error": "Configuration not found"}),
#             status_code=404,
#             mimetype="application/json"
#         )
#     except Exception as e:
#         logging.error(f"Error retrieving configuration: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to retrieve configuration: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )

# @app.route(route="config", methods=["POST"])
# def create_configuration(req: func.HttpRequest) -> func.HttpResponse:
#     """Create or update a storage account configuration."""
#     try:
#         req_body = req.get_json()
        
#         # Validate required fields
#         required_fields = ['storage_account_name', 'connection_string']
#         for field in required_fields:
#             if field not in req_body:
#                 return func.HttpResponse(
#                     json.dumps({"error": f"Missing required field: {field}"}),
#                     status_code=400,
#                     mimetype="application/json"
#                 )
        
#         # Prepare configuration document
#         config = {
#             'id': req_body['storage_account_name'],
#             'storage_account_name': req_body['storage_account_name'],
#             'connection_string': req_body['connection_string'],
#             'containers': req_body.get('containers', []),  # Empty list means monitor all containers
#             'enabled': req_body.get('enabled', True),
#             'created_at': datetime.utcnow().isoformat(),
#             'updated_at': datetime.utcnow().isoformat()
#         }
        
#         # Use storage account name as partition key
#         config_container.upsert_item(body=config)
        
#         return func.HttpResponse(
#             json.dumps({"message": "Configuration created/updated successfully", "config": config}),
#             status_code=201,
#             mimetype="application/json"
#         )
        
#     except ValueError as e:
#         return func.HttpResponse(
#             json.dumps({"error": "Invalid JSON in request body"}),
#             status_code=400,
#             mimetype="application/json"
#         )
#     except Exception as e:
#         logging.error(f"Error creating configuration: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to create configuration: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )

# @app.route(route="config/{storage_account_name}", methods=["PUT"])
# def update_configuration(req: func.HttpRequest) -> func.HttpResponse:
#     """Update an existing storage account configuration."""
#     try:
#         storage_account_name = req.route_params.get('storage_account_name')
#         req_body = req.get_json()
        
#         # Get existing configuration
#         try:
#             existing_config = config_container.read_item(
#                 item=storage_account_name,
#                 partition_key=storage_account_name
#             )
#         except exceptions.CosmosResourceNotFoundError:
#             return func.HttpResponse(
#                 json.dumps({"error": "Configuration not found"}),
#                 status_code=404,
#                 mimetype="application/json"
#             )
        
#         # Update fields if provided
#         if 'connection_string' in req_body:
#             existing_config['connection_string'] = req_body['connection_string']
#         if 'containers' in req_body:
#             existing_config['containers'] = req_body['containers']
#         if 'enabled' in req_body:
#             existing_config['enabled'] = req_body['enabled']
        
#         existing_config['updated_at'] = datetime.utcnow().isoformat()
        
#         # Save updated configuration
#         config_container.upsert_item(body=existing_config)
        
#         return func.HttpResponse(
#             json.dumps({"message": "Configuration updated successfully", "config": existing_config}),
#             status_code=200,
#             mimetype="application/json"
#         )
        
#     except ValueError as e:
#         return func.HttpResponse(
#             json.dumps({"error": "Invalid JSON in request body"}),
#             status_code=400,
#             mimetype="application/json"
#         )
#     except Exception as e:
#         logging.error(f"Error updating configuration: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to update configuration: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )

# @app.route(route="config/{storage_account_name}/toggle", methods=["POST"])
# def toggle_configuration(req: func.HttpRequest) -> func.HttpResponse:
#     """Toggle enabled/disabled status of a storage account configuration."""
#     try:
#         storage_account_name = req.route_params.get('storage_account_name')
        
#         # Get existing configuration
#         try:
#             config = config_container.read_item(
#                 item=storage_account_name,
#                 partition_key=storage_account_name
#             )
#         except exceptions.CosmosResourceNotFoundError:
#             return func.HttpResponse(
#                 json.dumps({"error": "Configuration not found"}),
#                 status_code=404,
#                 mimetype="application/json"
#             )
        
#         # Toggle enabled status
#         config['enabled'] = not config.get('enabled', False)
#         config['updated_at'] = datetime.utcnow().isoformat()
        
#         # Save updated configuration
#         config_container.upsert_item(body=config)
        
#         status = "enabled" if config['enabled'] else "disabled"
#         return func.HttpResponse(
#             json.dumps({
#                 "message": f"Configuration {status} successfully",
#                 "storage_account_name": storage_account_name,
#                 "enabled": config['enabled']
#             }),
#             status_code=200,
#             mimetype="application/json"
#         )
        
#     except Exception as e:
#         logging.error(f"Error toggling configuration: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to toggle configuration: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )

# @app.route(route="config/{storage_account_name}", methods=["DELETE"])
# def delete_configuration(req: func.HttpRequest) -> func.HttpResponse:
#     """Delete a storage account configuration."""
#     try:
#         storage_account_name = req.route_params.get('storage_account_name')
        
#         config_container.delete_item(
#             item=storage_account_name,
#             partition_key=storage_account_name
#         )
        
#         return func.HttpResponse(
#             json.dumps({"message": "Configuration deleted successfully"}),
#             status_code=200,
#             mimetype="application/json"
#         )
        
#     except exceptions.CosmosResourceNotFoundError:
#         return func.HttpResponse(
#             json.dumps({"error": "Configuration not found"}),
#             status_code=404,
#             mimetype="application/json"
#         )
#     except Exception as e:
#         logging.error(f"Error deleting configuration: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to delete configuration: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )

# @app.route(route="metadata", methods=["GET"])
# def get_blob_metadata(req: func.HttpRequest) -> func.HttpResponse:
#     """Get blob metadata with optional filtering."""
#     try:
#         # Get query parameters
#         storage_account = req.params.get('storage_account')
#         container = req.params.get('container')
#         limit = int(req.params.get('limit', 100))
        
#         # Build query
#         query = "SELECT * FROM c"
#         conditions = []
        
#         if storage_account:
#             conditions.append(f"c.storage_account_name = '{storage_account}'")
#         if container:
#             conditions.append(f"c.container_name = '{container}'")
        
#         if conditions:
#             query += " WHERE " + " AND ".join(conditions)
        
#         query += f" ORDER BY c.processed_at DESC OFFSET 0 LIMIT {limit}"
        
#         # Execute query
#         metadata_items = list(metadata_container.query_items(query=query, enable_cross_partition_query=True))
        
#         return func.HttpResponse(
#             json.dumps(metadata_items, indent=2),
#             status_code=200,
#             mimetype="application/json"
#         )
        
#     except Exception as e:
#         logging.error(f"Error retrieving metadata: {str(e)}")
#         return func.HttpResponse(
#             json.dumps({"error": f"Failed to retrieve metadata: {str(e)}"}),
#             status_code=500,
#             mimetype="application/json"
#         )


# def get_storage_account_config() -> Dict[str, Any]:
#     """Retrieve active storage account configurations from Cosmos DB."""
#     try:
#         # Query for active configurations
#         query = "SELECT * FROM c WHERE c.enabled = true"
#         configs = list(config_container.query_items(query=query, enable_cross_partition_query=True))
        
#         # Convert to dictionary for easier lookup
#         config_dict = {}
#         for config in configs:
#             account_name = config.get('storage_account_name')
#             if account_name:
#                 config_dict[account_name] = {
#                     'connection_string': config.get('connection_string'),
#                     'containers': config.get('containers', []),
#                     'enabled': config.get('enabled', False)
#                 }
        
#         return config_dict
#     except exceptions.CosmosResourceNotFoundError:
#         logging.warning("Configuration container not found")
#         return {}
#     except Exception as e:
#         logging.error(f"Error retrieving configuration: {str(e)}")
#         return {}

def should_process_blob(storage_account_name: str, container_name: str) -> bool:
    """Check if we should process blobs from this storage account and container."""
    return True  # For now, we process all blobs

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


@app.function_name(name="eventgridtrigger")
@app.event_grid_trigger(arg_name="azeventgrid")
def blob_created_handler(azeventgrid: func.EventGridEvent):
    """Handle blob created events from Event Grid."""
    try:
        # Parse event data
        event_data = azeventgrid.get_json()
        print(f"Received event: {json.dumps(event_data, indent=2)}")
        
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
        
        logging.info(f"Processing blob from storage account: {storage_account_name}, container: {container_name}, event: {event_data}")
        
        # Check if we should process this blob
        if not should_process_blob(storage_account_name, container_name):
            logging.info(f"Skipping blob processing for {storage_account_name}/{container_name} - not configured or disabled")
            return
        
        # Extract blob metadata
        metadata = extract_blob_metadata(
            blob_url=blob_url,
            storage_account_name=storage_account_name
        )
        
        if not metadata:
            logging.error("Failed to extract blob metadata")
            return
        
        # # Save to Cosmos DB
        # if save_metadata_to_cosmos(metadata):
        #     logging.info(f"Successfully processed blob: {metadata['blob_name']}")
        # else:
        #     logging.error(f"Failed to save metadata for blob: {metadata['blob_name']}")
            
    except Exception as e:
        logging.error(f"Error processing blob event: {str(e)}")
        raise
    