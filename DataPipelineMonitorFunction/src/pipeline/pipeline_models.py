import random
from datetime import datetime, timedelta
import uuid
import os
import sys

# --- START OF MANUAL PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
# --- END OF MANUAL PATH FIX ---

from src.config import constants

class PipelineRunResult:
    def __init__(self, pipeline_name: str, success: bool,
                 error_category: str = None, error_message: str = None,
                 duration_seconds: int = None, is_dlq: bool = False,
                 dlq_event_id: uuid.UUID = None,
                 start_timestamp: datetime = None, end_timestamp: datetime = None):
        self.pipeline_name = pipeline_name
        self.success = success
        self.error_category = error_category
        self.error_message = error_message
        self.duration_seconds = duration_seconds
        self.is_dlq = is_dlq
        self.dlq_event_id = dlq_event_id
        self.start_timestamp = start_timestamp if start_timestamp else datetime.now()
        self.end_timestamp = end_timestamp

class Pipeline:
    def __init__(self, name: str, team: str, description: str):
        self.name = name
        self.team = team
        self.description = description

    def execute(self, attempt_number: int = 1) -> PipelineRunResult:
        start_time = datetime.now()
        result = PipelineRunResult(pipeline_name=self.name, success=True, start_timestamp=start_time)

        if random.random() < constants.FAILURE_RATE:
            result.success = False
            result.error_category, result.error_message = self._get_random_error()

        if result.success:
            time_taken = random.randint(10, 60)
        else:
            time_taken = random.randint(30, 120)

        result.duration_seconds = time_taken
        result.end_timestamp = start_time + timedelta(seconds=time_taken)

        return result

    def _get_random_error(self):
        error_category = random.choice(constants.ERROR_CATEGORIES)

        if error_category == constants.ERROR_CATEGORY_CONNECTION:
            message = random.choice([
                "Database connection timed out.",
                "Network unreachable to source API.",
                "Failed to authenticate with data lake."
            ])
        elif error_category == constants.ERROR_CATEGORY_VALIDATION:
            message = random.choice([
                "Input data failed schema validation.",
                "Required field 'user_id' is missing.",
                "Invalid date format in 'transaction_date'."
            ])
        elif error_category == constants.ERROR_CATEGORY_DEPENDENCY:
            message = random.choice([
                "External pricing service is down.",
                "Third-party API returned 500.",
                "Data source system is unavailable."
            ])
        elif error_category == constants.ERROR_CATEGORY_RESOURCELIMIT:
             message = random.choice([
                "Memory limit exceeded during data processing.",
                "Disk space insufficient on processing node.",
                "Concurrent connections limit reached."
            ])
        else:
            message = f"Simulated {error_category} error."

        return error_category, message