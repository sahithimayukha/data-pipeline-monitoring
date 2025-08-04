# DataPipelineMonitorFunction/src/database/db_manager.py

import pyodbc
import os
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self):
        self.server = os.getenv('AZURE_SQL_SERVER')
        self.database = os.getenv('AZURE_SQL_DATABASE')
        self.username = os.getenv('AZURE_SQL_USERNAME')
        self.password = os.getenv('AZURE_SQL_PASSWORD')
        self.driver = '{ODBC Driver 17 for SQL Server}'

        if not all([self.server, self.database, self.username, self.password]):
            logger.error("Database credentials not fully set in environment variables.")
            raise ValueError("Missing database configuration.")

        self.conn_str = (
            f'DRIVER={self.driver};'
            f'SERVER=tcp:{self.server},1433;'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password};'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;'
        )
        self.cnxn = None

    def connect(self):
        try:
            self.cnxn = pyodbc.connect(self.conn_str)
            self.cnxn.autocommit = False
            logger.info("Successfully connected to Azure SQL Database.")
            return self.cnxn
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            error_message = f"Database connection error: {ex}"
            if sqlstate == '28000':
                logger.error(f"Authentication error. Check username, password, and server firewall rules. {error_message}")
            elif sqlstate == '08001':
                logger.error(f"Connection error. Check server name, IP firewall rules, and driver installation. {error_message}")
            else:
                logger.error(f"{error_message}")
            self.cnxn = None
            return None

    def close_connection(self):
        if self.cnxn:
            try:
                self.cnxn.close()
                logger.info("Database connection closed.")
            except pyodbc.Error as ex:
                logger.error(f"Error closing database connection: {ex}")

    def execute_query(self, query, params=None, commit=True):
        if not self.cnxn:
            logger.error("Not connected to the database. Cannot execute query.")
            return None

        cursor = None
        try:
            cursor = self.cnxn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if commit:
                self.cnxn.commit()
            logger.debug(f"Query executed successfully: {query[:100]}...")
            return cursor
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            logger.error(f"SQL Error executing query: {query[:100]}... Error: {ex}")
            self.cnxn.rollback()
            if cursor:
                cursor.close()
            return None
        finally:
            if cursor and not query.strip().upper().startswith("SELECT"):
                cursor.close()

    def create_tables(self):
        logger.info("Ensuring database tables exist and are populated...")

        if not self.cnxn:
            logger.error("Database connection not established. Cannot create tables.")
            return

        cursor = self.cnxn.cursor()

        try:
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimPipeline')
                CREATE TABLE DimPipeline (
                    pipeline_id INT PRIMARY KEY IDENTITY(1,1),
                    pipeline_name VARCHAR(255) NOT NULL UNIQUE,
                    team_name VARCHAR(100),
                    pipeline_description VARCHAR(500)
                );
            ''')
            logger.info("DimPipeline table ensured.")
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimStatus')
                CREATE TABLE DimStatus (
                    status_id INT PRIMARY KEY IDENTITY(1,1),
                    status_name VARCHAR(50) NOT NULL UNIQUE,
                    status_description VARCHAR(255)
                );
            ''')
            logger.info("DimStatus table ensured.")
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimError')
                CREATE TABLE DimError (
                    error_id INT PRIMARY KEY IDENTITY(1,1),
                    error_category VARCHAR(100) NOT NULL UNIQUE,
                    error_message_template VARCHAR(MAX)
                );
            ''')
            logger.info("DimError table ensured.")
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimTime')
                CREATE TABLE DimTime (
                    time_id INT PRIMARY KEY,
                    full_date DATE NOT NULL UNIQUE,
                    day_of_week INT,
                    day_name NVARCHAR(10),
                    month_name NVARCHAR(10),
                    month_num INT,
                    year_num INT,
                    quarter_num INT,
                    week_num INT
                );
            ''')
            logger.info("DimTime table ensured.")
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactPipelineRuns')
                CREATE TABLE FactPipelineRuns (
                    run_id BIGINT PRIMARY KEY IDENTITY(1,1),
                    pipeline_id INT NOT NULL,
                    status_id INT NOT NULL,
                    error_id INT,
                    time_id INT NOT NULL,
                    start_timestamp DATETIME NOT NULL,
                    end_timestamp DATETIME,
                    duration_seconds INT,
                    attempt_number INT NOT NULL DEFAULT 1,
                    raw_error_message VARCHAR(MAX),
                    is_dlq BIT NOT NULL DEFAULT 0,
                    dlq_event_id UNIQUEIDENTIFIER DEFAULT NEWID(),
                    FOREIGN KEY (pipeline_id) REFERENCES DimPipeline(pipeline_id),
                    FOREIGN KEY (status_id) REFERENCES DimStatus(status_id),
                    FOREIGN KEY (error_id) REFERENCES DimError(error_id),
                    FOREIGN KEY (time_id) REFERENCES DimTime(time_id)
                );
            ''')
            logger.info("FactPipelineRuns table ensured.")
            cursor.execute('''
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DLQEvents')
                CREATE TABLE DLQEvents (
                    dlq_event_id UNIQUEIDENTIFIER PRIMARY KEY,
                    pipeline_id INT NOT NULL,
                    event_timestamp DATETIME NOT NULL,
                    original_payload_json NVARCHAR(MAX),
                    error_details NVARCHAR(MAX),
                    replayed_at DATETIME,
                    dlq_status VARCHAR(50) NOT NULL DEFAULT 'Pending',
                    FOREIGN KEY (pipeline_id) REFERENCES DimPipeline(pipeline_id)
                );
            ''')
            logger.info("DLQEvents table ensured.")
            self.cnxn.commit()
            logger.info("All tables created/ensured successfully.")

            self._populate_dim_tables(cursor)
            self.cnxn.commit()
            logger.info("Dimension tables populated successfully.")

        except pyodbc.Error as ex:
            logger.error(f"Error during table creation or dimension population: {ex}")
            self.cnxn.rollback()

    def _populate_dim_tables(self, cursor):
        statuses = [
            ('Success', 'Pipeline run completed successfully.'),
            ('Failed', 'Pipeline run failed after all retries.'),
            ('Retrying', 'Pipeline run temporarily failed and is being retried.'),
            ('DLQ', 'Pipeline run failed permanently and moved to Dead Letter Queue.'),
            ('Skipped', 'Pipeline run was skipped (e.g., due to pre-check failure).')
        ]
        for status_name, status_desc in statuses:
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM DimStatus WHERE status_name = ?)
                INSERT INTO DimStatus (status_name, status_description) VALUES (?, ?)
            """, status_name, status_name, status_desc)
        logger.info("DimStatus populated.")
        pipelines = [
            ('UserImport', 'Data Ingestion', 'Ingests user profile data from external systems.'),
            ('SalesSync', 'Finance & Sales', 'Synchronizes sales records to the data warehouse.'),
            ('AnalyticsETL', 'Business Intelligence', 'Performs ETL for analytical dashboards.'),
            ('ProductUsageLog', 'Product Insights', 'Processes user interaction logs for feature analytics.'),
            ('InventoryUpdate', 'Supply Chain', 'Updates product inventory levels from supplier feeds.'),
            ('MarketingCampaign', 'Marketing', 'Processes marketing campaign performance data.')
        ]
        for p_name, t_name, p_desc in pipelines:
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM DimPipeline WHERE pipeline_name = ?)
                INSERT INTO DimPipeline (pipeline_name, team_name, pipeline_description) VALUES (?, ?, ?)
            """, p_name, p_name, t_name, p_desc)
        logger.info("DimPipeline populated.")
        error_categories = [
            ('Connection', 'Issue connecting to a database, API, or network resource.'),
            ('Validation', 'Data failed schema validation or business rule checks.'),
            ('Dependency', 'An external service or API dependency was unavailable or returned an error.'),
            ('Security', 'Authentication or authorization failure.'),
            ('BusinessRule', 'Data violated a critical business rule, preventing processing.'),
            ('Schema', 'Data schema mismatch with expected format.'),
            ('ResourceLimit', 'Exceeded compute, memory, disk, or network resource limits.'),
            ('Timeout', 'Operation timed out while waiting for a response.'),
            ('InvalidData', 'Input data was malformed or unexpected.'),
            ('Unknown', 'An unhandled or uncategorized error occurred.')
        ]
        for category, template in error_categories:
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM DimError WHERE error_category = ?)
                INSERT INTO DimError (error_category, error_message_template) VALUES (?, ?)
            """, category, category, template)
        logger.info("DimError populated.")
        start_date = datetime(datetime.now().year - 2, 1, 1)
        end_date = datetime(datetime.now().year + 2, 12, 31)
        delta = timedelta(days=1)
        current_date = start_date
        while current_date <= end_date:
            time_id = int(current_date.strftime('%Y%m%d'))
            full_date = current_date.date()
            day_of_week = current_date.isoweekday()
            day_name = current_date.strftime('%A')
            month_name = current_date.strftime('%B')
            month_num = current_date.month
            year_num = current_date.year
            quarter_num = (current_date.month - 1) // 3 + 1
            week_num = current_date.isocalendar()[1]
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM DimTime WHERE time_id = ?)
                INSERT INTO DimTime (time_id, full_date, day_of_week, day_name, month_name, month_num, year_num, quarter_num, week_num)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, time_id, time_id, full_date, day_of_week, day_name, month_name, month_num, year_num, quarter_num, week_num)
            current_date += delta
        logger.info("DimTime populated (for a range).")