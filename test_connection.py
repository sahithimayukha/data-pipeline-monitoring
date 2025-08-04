# test_blob_connection.py
import os
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    connection_str = os.getenv('AzureWebJobsStorage')
    container_name = os.getenv('AZURE_STAGING_CONTAINER')
    file_name = "test_file.txt"
    file_content = "This is a test message from a local script."

    if not connection_str or not container_name:
        logger.error("Blob storage credentials not set in .env file.")
        raise ValueError("Missing Blob Storage configuration.")

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_str)

    # Get a client for the container
    container_client = blob_service_client.get_container_client(container_name)

    # Create a blob client for the file
    blob_client = container_client.get_blob_client(file_name)

    # Upload the file
    blob_client.upload_blob(file_content, overwrite=True)

    logger.info(f"Successfully wrote a test file to the container: {container_name}")

except Exception as e:
    logger.error(f"Failed to write to Blob Storage: {e}", exc_info=True)