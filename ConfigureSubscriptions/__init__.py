import azure.functions as func
import json
import logging
from datetime import datetime
from azure.cosmos import CosmosClient, exceptions
import os
from typing import Dict, Any, List

# Reuse the same Cosmos client configuration
COSMOS_ENDPOINT = os.environ["COSMOS_ENDPOINT"]
COSMOS_KEY = os.environ["COSMOS_KEY"]
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "BlobMonitor")
CONFIG_CONTAINER_NAME = os.environ.get("CONFIG_CONTAINER_NAME", "Configuration")

cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
config_container = database.get_container_client(CONFIG_CONTAINER_NAME)

@app.route(route="config", methods=["GET"])
def get_configurations(req: func.HttpRequest) -> func.HttpResponse:
    """Get all storage account configurations."""
    try:
        # Query all configurations
        query = "SELECT * FROM c"
        configs = list(config_container.query_items(query=query, enable_cross_partition_query=True))
        
        return func.HttpResponse(
            json.dumps(configs, indent=2),
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

@app.route(route="config/{storage_account_name}", methods=["GET"])
def get_configuration(req: func.HttpRequest) -> func.HttpResponse:
    """Get configuration for a specific storage account."""
    try:
        storage_account_name = req.route_params.get('storage_account_name')
        
        config = config_container.read_item(
            item=storage_account_name,
            partition_key=storage_account_name
        )
        
        return func.HttpResponse(
            json.dumps(config, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": "Configuration not found"}),
            status_code=404,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error retrieving configuration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to retrieve configuration: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="config", methods=["POST"])
def create_configuration(req: func.HttpRequest) -> func.HttpResponse:
    """Create or update a storage account configuration."""
    try:
        req_body = req.get_json()
        
        # Validate required fields
        required_fields = ['storage_account_name', 'connection_string']
        for field in required_fields:
            if field not in req_body:
                return func.HttpResponse(
                    json.dumps({"error": f"Missing required field: {field}"}),
                    status_code=400,
                    mimetype="application/json"
                )
        
        # Prepare configuration document
        config = {
            'id': req_body['storage_account_name'],
            'storage_account_name': req_body['storage_account_name'],
            'connection_string': req_body['connection_string'],
            'containers': req_body.get('containers', []),  # Empty list means monitor all containers
            'enabled': req_body.get('enabled', True),
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
        
        # Use storage account name as partition key
        config_container.upsert_item(body=config)
        
        return func.HttpResponse(
            json.dumps({"message": "Configuration created/updated successfully", "config": config}),
            status_code=201,
            mimetype="application/json"
        )
        
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error creating configuration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to create configuration: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="config/{storage_account_name}", methods=["PUT"])
def update_configuration(req: func.HttpRequest) -> func.HttpResponse:
    """Update an existing storage account configuration."""
    try:
        storage_account_name = req.route_params.get('storage_account_name')
        req_body = req.get_json()
        
        # Get existing configuration
        try:
            existing_config = config_container.read_item(
                item=storage_account_name,
                partition_key=storage_account_name
            )
        except exceptions.CosmosResourceNotFoundError:
            return func.HttpResponse(
                json.dumps({"error": "Configuration not found"}),
                status_code=404,
                mimetype="application/json"
            )
        
        # Update fields if provided
        if 'connection_string' in req_body:
            existing_config['connection_string'] = req_body['connection_string']
        if 'containers' in req_body:
            existing_config['containers'] = req_body['containers']
        if 'enabled' in req_body:
            existing_config['enabled'] = req_body['enabled']
        
        existing_config['updated_at'] = datetime.utcnow().isoformat()
        
        # Save updated configuration
        config_container.upsert_item(body=existing_config)
        
        return func.HttpResponse(
            json.dumps({"message": "Configuration updated successfully", "config": existing_config}),
            status_code=200,
            mimetype="application/json"
        )
        
    except ValueError as e:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error updating configuration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to update configuration: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="config/{storage_account_name}/toggle", methods=["POST"])
def toggle_configuration(req: func.HttpRequest) -> func.HttpResponse:
    """Toggle enabled/disabled status of a storage account configuration."""
    try:
        storage_account_name = req.route_params.get('storage_account_name')
        
        # Get existing configuration
        try:
            config = config_container.read_item(
                item=storage_account_name,
                partition_key=storage_account_name
            )
        except exceptions.CosmosResourceNotFoundError:
            return func.HttpResponse(
                json.dumps({"error": "Configuration not found"}),
                status_code=404,
                mimetype="application/json"
            )
        
        # Toggle enabled status
        config['enabled'] = not config.get('enabled', False)
        config['updated_at'] = datetime.utcnow().isoformat()
        
        # Save updated configuration
        config_container.upsert_item(body=config)
        
        status = "enabled" if config['enabled'] else "disabled"
        return func.HttpResponse(
            json.dumps({
                "message": f"Configuration {status} successfully",
                "storage_account_name": storage_account_name,
                "enabled": config['enabled']
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error toggling configuration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to toggle configuration: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="config/{storage_account_name}", methods=["DELETE"])
def delete_configuration(req: func.HttpRequest) -> func.HttpResponse:
    """Delete a storage account configuration."""
    try:
        storage_account_name = req.route_params.get('storage_account_name')
        
        config_container.delete_item(
            item=storage_account_name,
            partition_key=storage_account_name
        )
        
        return func.HttpResponse(
            json.dumps({"message": "Configuration deleted successfully"}),
            status_code=200,
            mimetype="application/json"
        )
        
    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": "Configuration not found"}),
            status_code=404,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error deleting configuration: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to delete configuration: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="metadata", methods=["GET"])
def get_blob_metadata(req: func.HttpRequest) -> func.HttpResponse:
    """Get blob metadata with optional filtering."""
    try:
        # Get query parameters
        storage_account = req.params.get('storage_account')
        container = req.params.get('container')
        limit = int(req.params.get('limit', 100))
        
        # Build query
        query = "SELECT * FROM c"
        conditions = []
        
        if storage_account:
            conditions.append(f"c.storage_account_name = '{storage_account}'")
        if container:
            conditions.append(f"c.container_name = '{container}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += f" ORDER BY c.processed_at DESC OFFSET 0 LIMIT {limit}"
        
        # Execute query
        metadata_items = list(metadata_container.query_items(query=query, enable_cross_partition_query=True))
        
        return func.HttpResponse(
            json.dumps(metadata_items, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error retrieving metadata: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to retrieve metadata: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )