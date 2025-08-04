# DataPipelineMonitorFunction/src/pipeline/blob_data_writer.py
import os
import logging
import json
import uuid
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class BlobDataWriter:
    def __init__(self):
        self._staging_conn_str = os.getenv('AzureWebJobsStorage')
        self._staging_container = os.getenv('AZURE_STAGING_CONTAINER')

        if not self._staging_conn_str or not self._staging_container:
            raise ValueError("Staging blob storage not configured.")

        self._blob_service_client = BlobServiceClient.from_connection_string(self._staging_conn_str)

    def write_event(self, event_data: dict, pipeline_name: str):
        """Writes a single event to a JSON file in blob storage."""
        try:
            file_name = f"{pipeline_name}/{uuid.uuid4()}.json"
            blob_client = self._blob_service_client.get_blob_client(
                container=self._staging_container, blob=file_name
            )
            event_json_str = json.dumps(event_data)
            blob_client.upload_blob(event_json_str, overwrite=True)
            logger.info(f"Successfully wrote event to blob: {file_name}")
        except Exception as e:
            logger.error(f"Failed to write to blob storage: {e}")
            raise