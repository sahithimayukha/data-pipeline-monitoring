# 📊 Data Pipeline Monitoring & Health Dashboard

## Project Overview

This project is a complete, **end-to-end automated data analytics pipeline** that simulates, monitors, and analyzes the health of data pipelines. It adds crucial reliability features like automatic retries with exponential backoff and a Dead Letter Queue (DLQ) to handle permanent failures, transforming raw log data into a structured star schema for efficient reporting.

---

## 🔧 Key Features & Components:

✅ **Real-Time Data Extraction & Initial Transformation (Python & Azure Functions)** - Simulates pipeline runs with random failures, automatic retries with exponential backoff, and a Dead Letter Queue (DLQ) for permanent failures. 
- A Python Producer script (`src/producer/producer.py`) sends pipeline event data to a real-time stream. 
- An **Azure Function** (`DataPipelineMonitorFunction/function_app.py`) is triggered by this stream to process and write the raw event data (CSV files) to Azure Blob Storage. 
- Includes a **scheduled Timer Trigger** to run a daily monitoring batch job.

✅ **Automated Data Warehousing (Azure Data Factory & Azure SQL Database)** - Azure Blob Storage acts as a central landing zone for processed data files. 
- **Azure Data Factory (ADF)** orchestrates the daily ingestion of data from Blob Storage into Azure SQL Database. 
- Implements a **Star Schema** within Azure SQL Database, including: 
  - **Dimension Tables:** `DimPipeline`, `DimStatus`, `DimError`, `DimTime` 
  - **Fact Table:** `FactPipelineRuns` 
- Uses a two-step ADF pipeline with a `Copy Data` activity to a staging table and a `Stored Procedure` activity to perform dimension lookups and load the final fact table.

✅ **Automated Monitoring & Robustness (Azure Monitor)** - The system itself is designed for robustness with features like: 
- **Exponential Backoff** for retries. 
- A **Dead Letter Queue (DLQ)** to handle data from permanently failed jobs. 
- **Automated alert rules** can be configured in Azure Monitor to notify on critical Function App pipeline failures.

✅ **Power BI Dashboard & Service Deployment** - Connects to the star schema in Azure SQL Database for interactive reporting. 
- Features dynamic KPI cards (Success Rate, Failed Runs, DLQ Count). 
- Visualizes time-series trends (Pipeline Run Status over Time). 
- Displays distribution (Failures by Error Category). 
- Highlights top-failing pipelines. 
- **Published to Power BI Service** for broader sharing and automated refreshes.

---

## 🧱 Tech Stack

-   **Python 3.9+** – Data simulation, transformation, and scripting for Azure Function.
-   **Azure Functions** – Serverless compute for real-time and scheduled data ingestion.
-   **Azure Event Hubs** – Real-time data streaming service for pipeline events.
-   **Azure Blob Storage** – Scalable data lake for intermediate processed files (staging).
-   **Azure Data Factory** – Cloud-native ETL/ELT service for data warehousing orchestration.
-   **Azure SQL Database** – Relational cloud database hosting the star schema.
-   **Power BI** – Business intelligence visualization and reporting.

---

## 📌 Key Metrics Tracked

-   Total Pipeline Runs, Success Rate (%), Failed Runs
-   DLQ Events Count
-   Retries per Pipeline
-   Pipeline Run Status over Time
-   Failure Distribution by Error Category (e.g., Connection, Schema)
-   Top Failing Pipelines

---

## 🚀 Setup and Deployment

This section provides a high-level overview of the deployment steps. Refer to the specific code files and Azure Portal for detailed configuration.

### 1. Prerequisites

Ensure you have the following installed and configured before starting:

-   An **Azure Subscription**
-   **Azure CLI** installed and authenticated (`az login`)
-   **Azure Functions Core Tools v4.x** installed (`func --version`)
-   **Python 3.9+** installed locally
-   **SQL Server Management Studio (SSMS)** or **Azure Data Studio**
-   **Power BI Desktop**
-   **VS Code with Azure Functions and Python extensions**

### 2. Azure Resource Provisioning

Provision the necessary Azure infrastructure.

-   **Purpose:** To create the cloud services your pipeline will run on.
-   **Action:**
    1.  Open PowerShell (Windows) or Bash/Zsh (Linux/macOS).
    2.  Use the Azure Portal to manually create your:
        * **Resource Group** (`rg-pipemonitoring-dev`).
        * **Azure Storage Account** (`pipelinemonitorstore`).
        * **Azure Event Hubs Namespace** and **Event Hub** (`pipeline-events`).
        * **Azure SQL Server** and a **new database** (`pipemonitordb`).
        * **Azure Data Factory** instance (`pipemonitordf`).
    3.  Configure all firewall rules to allow Azure services and your local IP.
    4.  Note down all connection strings and credentials.

### 3. Database Schema Deployment

Deploy the data warehouse schema to your Azure SQL Database.

-   **Purpose:** To create the tables and stored procedures for your star schema.
-   **Action:**
    1.  Connect to your Azure SQL Database using SSMS.
    2.  Execute the provided SQL DDL scripts to create all dimension tables, fact tables, staging tables (`FactPipelineRuns_Staging`), and the stored procedure (`sp_LoadFactPipelineRuns`).

### 4. Azure Function App Deployment

Deploy your Python code to your Azure Function App.

-   **Purpose:** To automate the real-time processing and scheduled monitoring.
-   **Action:**
    1.  **Configure Local Secrets:** Populate your `.env` and `local.settings.json` files with all your sensitive details.
    2.  **Configure Azure App Settings:** Go to your Function App in the Azure Portal (`Configuration` -> `Application settings`). Add/set the corresponding environment variables.
    3.  **Deploy Code from VS Code:**
        * Open the `DataPipelineMonitorFunction/` folder in Visual Studio Code.
        * Install Python dependencies: `pip install -r requirements.txt`.
        * Use the Azure Functions extension in VS Code to deploy the project to your Azure Function App.
    4.  **Configure Timer Trigger Schedule:** The `function_app.py` has a timer trigger set to `0 30 3 * * *` (3:30 AM UTC, 9:00 AM IST) which will run automatically.

### 5. Azure Data Factory Pipeline Deployment & Schedule

Set up ADF to move data from Blob Storage to your SQL star schema.

-   **Purpose:** To automate the loading of data from Blob Storage into your data warehouse.
-   **Action:**
    1.  **Launch ADF Studio:** From your Azure Data Factory resource, click "Launch Studio".
    2.  **Create Linked Services:** Create connections to your Blob Storage (`AzureBlobStaging`) and Azure SQL Database (`AzureSqlMonitor`).
    3.  **Create Datasets:** Create datasets for your CSV files (`CsvMonitoringData`) and your SQL staging table (`SqlStaging`).
    4.  **Build Pipeline:** Create a pipeline (`PipelineMonitoringLoad`) with a `Copy Data` activity (from Blob to staging table) and a `Stored Procedure` activity (from staging table to final fact table).
    5.  **Schedule the Pipeline:** You can set up a schedule trigger to run the pipeline automatically after the scheduled Function App runs, or you can trigger it manually.

### 6. Power BI Dashboard Integration

Connect Power BI Desktop to your star schema and build your dashboard.

-   **Purpose:** To visualize your pipeline health data.
-   **Action:**
    1.  Open your Power BI Desktop.
    2.  Connect to your Azure SQL Database using `DirectQuery` mode.
    3.  Select your dimension and fact tables (`DimPipeline`, `DimStatus`, `DimError`, `DimTime`, `FactPipelineRuns`).
    4.  Verify the relationships and build your dashboard visuals.
    5.  **Publish your report to Power BI Service** for sharing and automated refresh configuration.

### 7. Monitoring & Alerting Setup

Set up alerts to get notified of pipeline failures.

-   **Purpose:** To ensure operational reliability of your automated pipeline.
-   **Action:**
    1.  Go to your Azure Function App in the Portal (`Overview` page -> `Application Insights` link).
    2.  If Application Insights is not linked, configure it.
    3.  In Application Insights (`Monitor` -> `Alerts` -> `+ Create` -> `Alert rule`).
    4.  **Condition:** Select signal `daily_youtube_pipeline_trigger Failures`. Configure it to alert if the `Count` is `Greater than` `0` over `5 minutes`.
    5.  **Actions:** Create an Action Group (e.g., `ag-YouTubePipelineFailures`) to send email/SMS notifications to yourself.
    6.  **Details:** Name your alert rule (e.g., `alert-daily-youtube-pipeline-failures`).
    7.  **Create** the alert rule.

---

## 👤 Author

**Sahithi Mayukha Najana** 📅 August 2025

---

## 📈 Future Enhancements

-   **Advanced Error Handling & Robustness:** Implement more sophisticated logging (e.g., custom log tables in SQL, structured JSON logs) and error recovery mechanisms within ADF and the Function App.
-   **Data Quality Checks:** Add explicit data quality validation activities in ADF or SQL stored procedures to ensure data integrity.
-   **Incremental Loading:** Optimize ADF pipelines for incremental loading of fact tables (e.g., using watermarks) rather than always processing all staged data, for efficiency with very large datasets.
-   **Expand Data Sources:** Integrate data from other YouTube APIs (e.g., YouTube Analytics API for deeper audience insights) or other platforms.
-   **Automate CI/CD:** Implement a Continuous Integration/Continuous Deployment (CI/CD) pipeline (e.g., using GitHub Actions or Azure DevOps) for automated code and infrastructure deployments.
-   **External Reporting Integration:** Set up automated email reporting using services like Power Automate or Azure Logic Apps to deliver daily/weekly summaries.