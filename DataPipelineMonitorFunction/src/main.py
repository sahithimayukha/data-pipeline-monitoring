import logging
import time
import random
import sys
import json
import uuid
import os
import csv
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# --- START OF MANUAL PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
# --- END OF MANUAL PATH FIX ---

from src.pipeline.pipeline_models import Pipeline, PipelineRunResult
from src.config import constants

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self):
        # Get the connection string from the existing AzureWebJobsStorage setting
        self._staging_conn_str = os.getenv('AzureWebJobsStorage')
        self._staging_container = os.getenv('AZURE_STAGING_CONTAINER')

        if not self._staging_conn_str or not self._staging_container:
            raise ValueError("Staging blob storage not configured.")

        self._blob_service_client = BlobServiceClient.from_connection_string(self._staging_conn_str)
        self._pipelines = [Pipeline(**p) for p in constants.PIPELINES]
        logger.info("Pipeline orchestrator initialized for blob storage.")

    def _write_to_blob(self, event_data: dict, file_name: str):
        """Writes a single event to a CSV file in blob storage with a simple folder structure."""
        try:
            # The file path is now just the container name and the file name
            full_path = file_name
            blob_client = self._blob_service_client.get_blob_client(
                container=self._staging_container, blob=full_path
            )

            # Write a single row as CSV
            import io
            output = io.StringIO()
            writer = csv.writer(output)

            writer.writerow(event_data.keys())
            writer.writerow(event_data.values())

            blob_client.upload_blob(output.getvalue(), overwrite=True)
            logger.info(f"Successfully wrote event to blob: {full_path}")
        except Exception as e:
            logger.error(f"Failed to write to blob storage: {e}")
            raise

    def process_event(self, event_data: dict):
        """Processes a single event and writes it to staging."""
        pipeline_name = event_data.get("pipeline_name")
        if not pipeline_name:
            logger.error("Event received without a 'pipeline_name'. Skipping.")
            return

        file_name = f"{pipeline_name}/{uuid.uuid4()}.csv"
        self._write_to_blob(event_data, file_name)

    def run_continuous_simulation(self, total_runs=100):
        """Simulates pipeline runs and handles max_attempts logic."""
        logger.info(f"Starting continuous simulation for {total_runs} total runs.")
        run_count = 0

        while run_count < total_runs:
            pipeline = random.choice(self._pipelines)
            attempt_number = 1

            while attempt_number <= constants.MAX_ATTEMPTS:
                run_result = pipeline.execute(attempt_number=attempt_number)

                if run_result.success:
                    event_data = {
                        "pipeline_name": pipeline.name,
                        "success": run_result.success,
                        "start_timestamp": run_result.start_timestamp.isoformat(),
                        "end_timestamp": run_result.end_timestamp.isoformat(),
                        "duration_seconds": run_result.duration_seconds,
                        "error_category": run_result.error_category,
                        "error_message": run_result.error_message,
                        "attempt_number": attempt_number,
                        "is_dlq": False
                    }
                    self.process_event(event_data, attempt_number)
                    break
                else:
                    if attempt_number == constants.MAX_ATTEMPTS:
                        event_data = {
                            "pipeline_name": pipeline.name,
                            "success": run_result.success,
                            "start_timestamp": run_result.start_timestamp.isoformat(),
                            "end_timestamp": run_result.end_timestamp.isoformat(),
                            "duration_seconds": run_result.duration_seconds,
                            "error_category": run_result.error_category,
                            "error_message": run_result.error_message,
                            "attempt_number": attempt_number,
                            "is_dlq": True
                        }
                        self.process_event(event_data, attempt_number)
                        break
                    wait_time = constants.BASE_BACKOFF_TIME_SECONDS * (2 ** (attempt_number - 1))
                    jitter = random.uniform(0, 1) * wait_time * 0.1
                    sleep_duration = wait_time + jitter
                    time.sleep(sleep_duration)

                attempt_number += 1

            run_count += 1
            time.sleep(random.uniform(2, 10))

        logger.info(f"Continuous simulation of {total_runs} runs completed.")