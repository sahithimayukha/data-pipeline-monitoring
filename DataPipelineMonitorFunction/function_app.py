import logging
import json
import sys
import os
from datetime import datetime, timedelta

import azure.functions as func

# This is a critical step to ensure our function can find the 'src' package.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.main import PipelineOrchestrator

# Define a Function App instance
app = func.FunctionApp()

@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="pipeline-events",
    connection="AzureWebJobsEventHubsConnection",
    dataType="string"
)
def ProcessPipelineEvent(event: func.EventHubEvent):
    logging.info('Python EventHub trigger function processed an event.')

    try:
        orchestrator = PipelineOrchestrator()

        for event_data in event:
            try:
                event_body = event_data.get_body().decode('utf-8')
                event_json = json.loads(event_body)

                orchestrator.process_event(event_json)

            except Exception as e:
                logging.error(f"Error processing single event: {e}", exc_info=True)

    except Exception as e:
        logging.critical(f"An unhandled error occurred in the Azure Function: {e}", exc_info=True)

@app.timer_trigger(schedule="0 30 3 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def ScheduledMonitor(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    try:
        orchestrator = PipelineOrchestrator()
        orchestrator.run_continuous_simulation(total_runs=50)

    except Exception as e:
        logging.critical(f"An unhandled error occurred in the scheduled function: {e}", exc_info=True)