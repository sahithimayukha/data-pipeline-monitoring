import logging
import time
import random
import json
import os
import sys
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# --- START OF MANUAL PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
# --- END OF MANUAL PATH FIX ---

load_dotenv()

from src.pipeline.pipeline_models import Pipeline
from src.config import constants

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineEventProducer:
    def __init__(self):
        self.connection_str = os.getenv('AZURE_EVENTHUB_CONNECTION_STRING')
        self.eventhub_name = os.getenv('AZURE_EVENTHUB_NAME')

        if not all([self.connection_str, self.eventhub_name]):
            logger.error("Event Hubs credentials not set in environment variables.")
            raise ValueError("Missing Event Hubs configuration.")

        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_str,
            eventhub_name=self.eventhub_name
        )

        self.pipelines = [Pipeline(**p) for p in constants.PIPELINES]
        logger.info("Pipeline event producer initialized.")

    def send_events(self, num_events: int):
        with self.producer:
            for i in range(num_events):
                pipeline_to_run = random.choice(self.pipelines)
                run_result = pipeline_to_run.execute(attempt_number=1)

                payload = {
                    "pipeline_name": pipeline_to_run.name,
                    "success": run_result.success,
                    "start_timestamp": run_result.start_timestamp.isoformat(),
                    "end_timestamp": run_result.end_timestamp.isoformat(),
                    "duration_seconds": run_result.duration_seconds,
                    "error_category": run_result.error_category,
                    "error_message": run_result.error_message
                }

                event_data_batch = self.producer.create_batch()
                event_data_batch.add(EventData(json.dumps(payload)))
                self.producer.send_batch(event_data_batch)
                logger.info(f"Sent event for pipeline '{pipeline_to_run.name}' (Success={run_result.success})")

                time.sleep(random.uniform(0.5, 2.0))

        logger.info(f"Finished sending {num_events} events.")

def main():
    try:
        producer = PipelineEventProducer()
        logger.info("Starting continuous event production...")
        while True:
            producer.send_events(num_events=1)
            time.sleep(1)

    except ValueError as e:
        logger.critical(f"Setup aborted: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()