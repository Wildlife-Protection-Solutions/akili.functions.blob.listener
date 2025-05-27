from dataclasses import dataclass
from enum import Enum
from typing import Optional

class CosmosDocumentType(Enum):
    """An enum for the cosmos document types."""
    FILE_HASH = "file_hash"
    DEPLOYMENT_METADATA = "deployment_metadata"
    
@dataclass
class DeploymentFileHashDocument:
    """ A class for deployment file hash documents in Cosmos DB."""
    file_hash: str  # This is the id in the Cosmos DB document
    deployment_id: int
    created_ms: int
    
    @property
    def type(self) -> str:
        """ Returns the type of the Cosmos document. """
        return CosmosDocumentType.FILE_HASH.value
    
    def from_dict(data: dict):
        """ Creates a DeploymentFileHashDocument from a dictionary.

        Args:
            data (dict): the dictionary to convert

        Returns:
            DeploymentFileHashDocument: the created document
        """
        assert data["type"] == CosmosDocumentType.FILE_HASH.value, "Invalid document type"
        return DeploymentFileHashDocument(
            file_hash=data["id"],
            deployment_id=int(data["deployment_id"]),
            created_ms=data["created_ms"]
        )
    
    def to_dict(self) -> dict:
        """ Converts the DeploymentFileHashDocument to a dictionary.

        Returns:
            dict: the dictionary representation of the document
        """
        return {
            "id": self.file_hash,
            "deployment_id": self.deployment_id,
            "created_ms": self.created_ms,
            "type": self.type,
        }
        
@dataclass
class DeploymentMetadataDocument:
    """ A class for deployment metadata documents in Cosmos DB."""
    deployment_id: int  # this is the id in the Cosmos DB document
    project_id: int
    hash_count: int
    upload_in_progress: bool
    upload_user_id: Optional[str]
    last_update_ms: int
    
    @property
    def type(self) -> str:
        """ Returns the type of the Cosmos document. """
        return CosmosDocumentType.DEPLOYMENT_METADATA.value

    def from_dict(data: dict):
        """ Creates a DeploymentMetadataDocument from a dictionary. 
        
        Args:
            data (dict): the dictionary to convert
            
        Returns:
            DeploymentMetadataDocument: the created document
        """
        assert data["type"] == CosmosDocumentType.DEPLOYMENT_METADATA.value, "Invalid document type"
        return DeploymentMetadataDocument(
            deployment_id=int(data["id"]),
            project_id=data["project_id"],
            hash_count=data["hash_count"],
            upload_in_progress=data["upload_in_progress"],
            upload_user_id=data["upload_user_id"],
            last_update_ms=data["last_update_ms"]
        )

    def to_dict(self) -> dict:
        """ Converts the DeploymentMetadataDocument to a dictionary.

        Returns:
            dict: the dictionary representation of the document
        """
        return {
            "id": str(self.deployment_id),
            "deployment_id": self.deployment_id,
            "project_id": self.project_id,
            "hash_count": self.hash_count,
            "upload_in_progress": self.upload_in_progress,
            "upload_user_id": self.upload_user_id,
            "last_update_ms": self.last_update_ms,
            "type": self.type,
        }