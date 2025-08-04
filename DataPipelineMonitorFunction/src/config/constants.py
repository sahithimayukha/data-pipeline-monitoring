# src/config/constants.py

# --- Pipeline Definitions ---
# These match the pipeline_name and team_name in DimPipeline
PIPELINES = [
    {"name": "UserImport", "team": "Data Ingestion", "description": "Ingests user profile data from external systems."},
    {"name": "SalesSync", "team": "Finance & Sales", "description": "Synchronizes sales orders and customer data."},
    {"name": "AnalyticsETL", "team": "Business Intelligence", "description": "Performs ETL for analytical dashboards."},
    {"name": "ProductUsageLog", "team": "Product Insights", "description": "Processes user interaction logs for feature analytics."},
    {"name": "InventoryUpdate", "team": "Supply Chain", "description": "Updates product inventory levels from supplier feeds."},
    {"name": "MarketingCampaign", "team": "Marketing", "description": "Processes marketing campaign performance data."}
]

# --- Status Definitions (Match DimStatus in DB) ---
STATUS_SUCCESS = "Success"
STATUS_FAILED = "Failed"
STATUS_RETRYING = "Retrying"
STATUS_DLQ = "DLQ"
STATUS_SKIPPED = "Skipped" # If we implement skipping logic later

# --- Error Categories (Match DimError in DB) ---
ERROR_CATEGORY_CONNECTION = "Connection"
ERROR_CATEGORY_VALIDATION = "Validation"
ERROR_CATEGORY_DEPENDENCY = "Dependency"
ERROR_CATEGORY_SECURITY = "Security"
ERROR_CATEGORY_BUSINESSRULE = "BusinessRule"
ERROR_CATEGORY_SCHEMA = "Schema"
ERROR_CATEGORY_RESOURCELIMIT = "ResourceLimit"
ERROR_CATEGORY_TIMEOUT = "Timeout"
ERROR_CATEGORY_INVALIDDATA = "InvalidData"
ERROR_CATEGORY_UNKNOWN = "Unknown"

ERROR_CATEGORIES = [
    ERROR_CATEGORY_CONNECTION,
    ERROR_CATEGORY_VALIDATION,
    ERROR_CATEGORY_DEPENDENCY,
    ERROR_CATEGORY_SECURITY,
    ERROR_CATEGORY_BUSINESSRULE,
    ERROR_CATEGORY_SCHEMA,
    ERROR_CATEGORY_RESOURCELIMIT,
    ERROR_CATEGORY_TIMEOUT,
    ERROR_CATEGORY_INVALIDDATA,
    ERROR_CATEGORY_UNKNOWN
]

# --- Simulation Parameters ---
FAILURE_RATE = 0.50 # 30% chance of a pipeline failing initially
MAX_ATTEMPTS = 3     # Max attempts before moving to DLQ (1 initial + 2 retries)

# Base time for exponential backoff (in seconds)
# The actual wait time will be BASE_BACKOFF_TIME * (2 ** (attempt - 1))
# E.g., 1st retry: 2^0 * 2s = 2s, 2nd retry: 2^1 * 2s = 4s, 3rd retry: 2^2 * 2s = 8s
BASE_BACKOFF_TIME_SECONDS = 2

# --- DLQ Statuses ---
DLQ_STATUS_PENDING = "Pending"
DLQ_STATUS_REPLAYED = "Replayed"
DLQ_STATUS_ARCHIVED = "Archived"
DLQ_STATUS_IGNORED = "Ignored"