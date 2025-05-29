# src/dal/deployment_cosmos_db_dal.py

from typing import Optional
from uuid import UUID

from datetime import datetime, timezone

from shared.cosmos_db_dal import BaseCosmosDBDAL
from shared.cosmos_documents import DeploymentFileHashDocument, DeploymentMetadataDocument


class DeploymentCosmosDBDAL(BaseCosmosDBDAL):
    """A class for interacting with the Cosmos DB container for deployment metadata."""
    
    def get_deployment_metadata(
        self, deployment_id: int
    ) -> Optional[DeploymentMetadataDocument]:
        """Get a deployment metadata item from the Cosmos DB container.

        Args:
            deployment_id (int): the id of the item to get

        Returns:
            DeploymentMetadata: the metadata if found, None otherwise
        """
        data = self.get_item(item_id=str(deployment_id), partition_key=deployment_id)
        return DeploymentMetadataDocument.from_dict(data) if data else None

    def add_deployment_metadata(
        self, deployment_id: int, project_id: int
    ) -> Optional[DeploymentMetadataDocument]:
        """Adds a new deployment item to the Cosmos DB container.

        Args:
            deployment_id (int): the id of the deployment item
            project_id (int): the id of the project

        Returns:
            DeploymentMetadata: the created item or None if the operation fails
        """
        if self.get_deployment_metadata(deployment_id):
            raise ValueError(
                "Deployment metadata already exists."
            )
        
        metadata = DeploymentMetadataDocument(
            deployment_id=deployment_id,
            project_id=project_id,
            hash_count=0,
            upload_in_progress=False,
            upload_user_id=None,
            last_update_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
        )
        item = self.add_item(metadata.to_dict())
        if item is None:
            raise ValueError("Failed to add deployment metadata: ", metadata.to_dict())
        return DeploymentMetadataDocument.from_dict(item)
    
    def add_file_hashes(
        self, deployment_id: int, file_hashes: list[str]
    ) -> Optional[bool]:
        """Add multiple file hashes in a bulk insert operation.

        Args:
            deployment_id (int): the id of the deployment item
            file_hashes (list[str]): the list of file hashes to add

        Returns:
            bool: True if the operation was successful
            
        Raises:
            ValueError: if the deployment metadata does not exist or the operation fails
        """
        # Create a list of DeploymentFileHashDocument objects
        file_hash_documents = [
            DeploymentFileHashDocument(
                file_hash=file_hash,
                deployment_id=deployment_id,
                created_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
            )
            for file_hash in file_hashes
        ]
        batch_operations = [
            ('create', (file_hash_document.to_dict(),))
            for file_hash_document in file_hash_documents
        ]
        # Update the deployment metadata
        metadata = self.get_deployment_metadata(deployment_id)
        if not metadata:
            raise ValueError("Deployment metadata does not exist.")
        metadata.hash_count += len(file_hashes)
        metadata.last_update_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        item_id = str(deployment_id)
        item_body = metadata.to_dict()
        batch_operations.append(
            ("replace", (item_id, item_body))
        )
        # Add the file hash documents and update the metadata in a batch operation
        results = self.execute_batch_items(batch_operations, partition_key=deployment_id)
        if not results:
            raise ValueError("Failed to add file hashes: ", file_hashes)
        # Return the updated metadata
        return True
    
    def set_upload_in_progress(
        self,
        deployment_id: int,
        upload_in_progress: bool,
        user_id: UUID = None,
    ) -> Optional[DeploymentMetadataDocument]:
        """Set the upload in progress status for a deployment item in the Cosmos DB container.

        Args:
            deployment_id (int): the id of the deployment item
            upload_in_progress (bool): the upload in progress status
            user_id (UUID): the user who is uploading the file

        Returns:
            DeploymentMetadata: the updated item if successful, None otherwise
        """
        metadata = self.get_deployment_metadata(deployment_id)
        if not metadata:
            raise ValueError("Deployment metadata does not exist.")
        
        metadata.upload_in_progress = upload_in_progress
        metadata.upload_user_id = str(user_id) if user_id else None
        metadata.last_update_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        updated_metadata = self.update_item(str(deployment_id), metadata.to_dict())
        if not updated_metadata:
            raise ValueError("Failed to update deployment metadata: ", metadata.to_dict())
        return DeploymentMetadataDocument.from_dict(updated_metadata)

    def delete_deployment_metadata(self, deployment_id: int):
        """Delete a deployment item from the Cosmos DB container.

        Args:
            deployment_id (int): the id of the deployment item

        Returns:
            None
        """
        self.delete_item(item_id=str(deployment_id), partition_key=deployment_id)

    def get_file_hashes(
        self, deployment_id: int, skip: int = 0, take: int = 100
    ) -> list[str]:
        """Get file hashes for a deployment item from the Cosmos DB container.

        Args:
            deployment_id (int): the id of the deployment item
            skip (int): the number of items to skip
            take (int): the number of items to take

        Returns:
            list[DeploymentFileHashDocument]: the file hashes if found, empty list otherwise
        """
        file_hashes = self.query_item_by_partition(
            partition_key_value=deployment_id,
            field_name="id",  # the id is the file hash in the Cosmos DB document
            additional_where="c.type = 'file_hash'",
            skip=skip,
            take=take,
        )
        return [file_hashes["id"] for file_hashes in file_hashes] if file_hashes else []
