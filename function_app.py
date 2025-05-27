import json
import logging
import azure.functions as func
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

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
