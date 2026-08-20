# GCP Healthcare Revenue Cycle Management (RCM) ETL Pipeline

An end-to-end **Healthcare Revenue Cycle Management (RCM) data engineering pipeline on Google Cloud Platform (GCP)**.

This project integrates data from multiple healthcare sources, including two hospital EMR systems, insurance claims, CPT codes, ICD-10 codes, and NPI provider data. The pipeline uses **Cloud SQL, Google Cloud Storage (GCS), Dataproc/PySpark, BigQuery, Cloud Composer (Airflow), and Cloud Build** to ingest, process, validate, transform, and prepare healthcare data for analytics.

> **Important:** The repository contains sample/demo configuration values. Before deploying, replace project IDs, bucket names, database endpoints, credentials, Composer bucket names, and other environment-specific values. Secrets should be stored in **Secret Manager** rather than committed to source control.

---

## 1. Project Overview

### Business Domain

The project models a simplified **Healthcare Revenue Cycle Management (RCM)** process:

1. A patient receives healthcare services.
2. The hospital records the patient, provider, department, encounter, and transaction information.
3. A claim is submitted to an insurance payor.
4. The claim can be approved, denied, rejected, or remain pending.
5. Payments are received from insurance/patients.
6. Revenue, outstanding balances, provider performance, department performance, and claim metrics are made available for analytics.

### Data Engineering Objective

The main objective is to build a scalable batch ETL pipeline that:

- Extracts healthcare data from multiple sources.
- Supports both **full and incremental ingestion**.
- Stores raw data in a GCS data lake.
- Archives previously processed landing files.
- Maintains audit and pipeline logs.
- Uses Dataproc/PySpark for ingestion and processing.
- Creates BigQuery Bronze, Silver, and Gold layers.
- Applies data-quality checks and quarantine flags.
- Implements **SCD Type 2-style history** in the Silver layer.
- Produces business-ready Gold tables for healthcare RCM analytics.
- Uses Airflow to orchestrate Dataproc and BigQuery workloads.
- Uses Cloud Build to automate deployment of DAGs and pipeline data to the Composer environment.

---

## 2. Architecture

```text
                         HEALTHCARE DATA SOURCES
                                  |
             +--------------------+--------------------+
             |                    |                    |
             v                    v                    v
      Cloud SQL / MySQL       Claims CSV         Reference APIs
      Hospital A + B          Insurance           NPI / ICD
             |                    |                    |
             |                    v                    |
             |              Google Cloud Storage       |
             |                 Landing Zone            |
             |                    ^                    |
             +----------+---------+--------------------+
                        |
                        v
                 Dataproc / PySpark
                        |
          +-------------+-------------+
          |             |             |
          v             v             v
      EMR/MySQL       Claims       Reference Data
      ingestion       ingestion    ingestion
          |
          v
             Google Cloud Storage
             Raw / Landing / Archive
                        |
                        v
                  BigQuery Bronze
              External/raw source layer
                        |
                        v
                  BigQuery Silver
             Cleansing + Data Quality
                    + SCD Type 2
                        |
                        v
                   BigQuery Gold
              Business/Analytics layer
                        |
          +-------------+-------------+
          |             |             |
          v             v             v
      Provider       Department     Financial/
      Analytics      Analytics      Claims KPIs


       Cloud Composer / Airflow
                 |
       +---------+---------+
       |                   |
       v                   v
  Dataproc DAG        BigQuery DAG
       |                   |
       +---------+---------+
                 |
                 v
          End-to-end workflow

             Cloud Build
                 |
                 v
       Deploy DAGs + pipeline
       data to Composer bucket
```

---

## 3. GCP Services Used

| Service | Purpose |
|---|---|
| **Cloud SQL / MySQL** | Source EMR databases for Hospital A and Hospital B |
| **Cloud Storage (GCS)** | Data lake landing, archive, configuration, temporary files, and pipeline logs |
| **Dataproc** | Runs PySpark jobs for EMR, claims, and reference-data processing |
| **BigQuery** | Audit logging, Bronze/Silver/Gold data warehouse, analytics tables |
| **Cloud Composer / Airflow** | Workflow orchestration and dependency management |
| **Cloud Build** | CI/CD-style deployment of DAGs and project data to Composer |
| **Python / PySpark** | Extraction, transformation, ingestion, and data processing |
| **REST APIs** | NPI Registry and WHO ICD API reference data |

---

## 4. Source Systems

### Hospital EMR Data

Two hospital branches are modeled as separate MySQL/Cloud SQL databases:

- `hospital_a_db`
- `hospital_b_db`

Each source contains:

- Patients
- Providers
- Departments
- Encounters
- Transactions

The two hospitals intentionally have slightly different patient schemas, demonstrating a common data-engineering scenario where source systems are not completely standardized.

### Insurance Claims

Claims arrive as CSV files and are stored in the GCS landing area.

The pipeline identifies the originating hospital from the source file path and adds a `datasource` attribute.

### CPT Codes

CPT (Current Procedural Terminology) reference data is supplied as CSV and loaded into the pipeline.

### ICD-10 Codes

ICD-10 reference data is retrieved from the WHO ICD API and written to GCS as Parquet.

### NPI Data

NPI provider reference data is retrieved from the CMS NPI Registry API and written to GCS as Parquet.

---

## 5. GCS Data Lake Layout

The project uses GCS as the landing/data-lake layer.

A representative structure is:

```text
gs://<healthcare-bucket>/
│
├── landing/
│   ├── hospital-a/
│   │   ├── patients/
│   │   ├── providers/
│   │   ├── departments/
│   │   ├── encounters/
│   │   ├── transactions/
│   │   └── archive/
│   │
│   ├── hospital-b/
│   │   ├── patients/
│   │   ├── providers/
│   │   ├── departments/
│   │   ├── encounters/
│   │   ├── transactions/
│   │   └── archive/
│   │
│   ├── claims/
│   ├── cptcodes/
│   ├── icd_codes/
│   └── npi_extract/
│
├── configs/
│   └── load_config.csv
│
└── temp/
    └── pipeline_logs/
```

---

## 6. Incremental and Full Load Strategy

The EMR ingestion is metadata/configuration driven using:

`data/configs/load_config.csv`

Example configuration:

```text
database,datasource,tablename,loadtype,watermark,is_active,targetpath
hospital-a-mysql-db,hospital_a_db,encounters,Incremental,ModifiedDate,1,hospital-a
hospital-a-mysql-db,hospital_a_db,patients,Incremental,ModifiedDate,1,hospital-a
hospital-a-mysql-db,hospital_a_db,transactions,Incremental,ModifiedDate,1,hospital-a
hospital-a-mysql-db,hospital_a_db,providers,Full,,1,hospital-a
hospital-a-mysql-db,hospital_a_db,departments,Full,,1,hospital-a
```

### Incremental Load

For incremental tables, the pipeline:

1. Reads the latest successful load timestamp from the BigQuery audit table.
2. Uses the configured watermark column, such as `ModifiedDate`.
3. Extracts records newer than the previous watermark.
4. Writes the extracted records to GCS.
5. Records the load count and status in the audit table.

Conceptually:

```text
Last successful watermark
          |
          v
SELECT * FROM source
WHERE ModifiedDate > last_watermark
          |
          v
      GCS Landing
```

### Full Load

For full-load tables such as providers and departments, the pipeline extracts the complete source table.

---

## 7. File Archiving

Before a new EMR extraction, existing landing JSON files are moved into an archive path.

Example:

```text
landing/hospital-a/archive/<table>/<year>/<month>/<day>/
```

This provides:

- Historical source snapshots
- Recovery capability
- Better traceability
- Separation between active landing data and previous loads

---

## 8. Dataproc / PySpark Processing

The Dataproc workflow runs the PySpark ingestion jobs.

### EMR Ingestion

- `hospitalA_mysqlToLanding.py`
- `hospitalB_mysqlToLanding.py`

These jobs:

1. Connect to MySQL through JDBC.
2. Read the configuration from GCS.
3. Determine full vs incremental load.
4. Extract source data.
5. Write JSON records to GCS.
6. Write audit information to BigQuery.
7. Save pipeline logs to GCS.
8. Save pipeline logs to BigQuery.

### Claims Ingestion

`claims.py`

Responsibilities:

- Read claims CSV files from GCS.
- Determine the hospital source.
- Remove duplicates.
- Write claims data to the BigQuery Bronze layer.

### CPT Ingestion

`cpt_codes.py`

Responsibilities:

- Read CPT CSV data from GCS.
- Normalize column names.
- Write the result to BigQuery Bronze.

### ICD/NPI Ingestion

The repository also contains:

- `icd_codes.py`
- `npi_codes.py`

These scripts call external healthcare reference APIs and write reference data to GCS in Parquet format.

---

## 9. BigQuery Medallion Architecture

The warehouse follows a three-layer architecture:

```text
GCS / Source Data
       |
       v
+------------------+
| Bronze Layer     |
| Raw / External   |
+------------------+
       |
       v
+------------------+
| Silver Layer     |
| Clean + Quality  |
| + SCD Type 2     |
+------------------+
       |
       v
+------------------+
| Gold Layer       |
| Business KPIs    |
| & Analytics      |
+------------------+
```

### Bronze Layer

The Bronze SQL creates BigQuery external tables over GCS JSON files for Hospital A and Hospital B.

Examples:

- `departments_ha`
- `encounters_ha`
- `patients_ha`
- `providers_ha`
- `transactions_ha`
- `departments_hb`
- `encounters_hb`
- `patients_hb`
- `providers_hb`
- `transactions_hb`

Claims and CPT ingestion jobs also populate Bronze BigQuery tables.

### Silver Layer

The Silver layer standardizes data from both hospital sources.

Major responsibilities include:

- Deduplication
- Source-system identification
- Data-quality checks
- Null validation
- Quarantine flagging
- Source-to-target key generation
- SCD Type 2-style history tracking

Common columns include:

```text
<entity>_Key
SRC_<entity>ID
datasource
is_quarantined
inserted_date
modified_date
is_current
```

This allows the pipeline to maintain the current version of an entity while retaining historical versions when source attributes change.

### Gold Layer

The Gold layer contains business-ready analytics tables.

Current project outputs include:

- `provider_charge_summary`
- `patient_history`
- `provider_performance`
- `department_performance`
- `financial_metrics`
- `payor_performance`

---

## 10. Data Quality and Quarantine

The Silver transformations contain quality rules such as:

- Required patient identifiers must be present.
- Required encounter identifiers must be present.
- Transaction identifiers must be present.
- Patient IDs must be present.
- Required dates must be present.
- Invalid/null encounter types can be quarantined.

Records are not necessarily discarded immediately. Instead, they can be marked using:

```text
is_quarantined = TRUE
```

This provides visibility into data-quality problems while preserving the original source data.

---

## 11. SCD Type 2 Implementation

The Silver layer uses BigQuery `MERGE` statements to maintain historical versions of records.

The general process is:

```text
Incoming Record
      |
      v
Does current record exist?
      |
   +--+--+
   |     |
  Yes    No
   |     |
Changed? Insert
   |
  Yes
   |
   v
Set previous record:
is_current = FALSE
   |
   v
Insert latest version:
is_current = TRUE
```

This approach is useful for tracking changes to entities such as:

- Patients
- Providers
- Departments
- Encounters
- Transactions

---

## 12. Audit and Logging

The project maintains operational metadata in BigQuery.

### Audit Table

The repository contains:

`data/configs/audit_table_ddl.sql`

The audit table tracks:

- Data source
- Table name
- Load type
- Record count
- Load timestamp
- Status

Example:

```text
data_source
tablename
load_type
record_count
load_timestamp
status
```

### Pipeline Logs

The EMR ingestion scripts also create structured pipeline logs and store them in:

```text
GCS:
temp/pipeline_logs/

BigQuery:
temp_dataset.pipeline_logs
```

This supports operational monitoring and troubleshooting.

---

## 13. Airflow / Cloud Composer Orchestration

The project contains three DAGs.

### `parent_dag.py`

The parent DAG is the main orchestration workflow.

It:

1. Triggers the PySpark/Dataproc DAG.
2. Waits for completion.
3. Triggers the BigQuery DAG.
4. Waits for completion.

The dependency is:

```text
parent_dag
    |
    v
pyspark_dag
    |
    v
bigquery_dag
```

The configured schedule is:

```text
0 5 * * *
```

which represents a daily 5:00 schedule in the Airflow environment's configured timezone.

### `pyspark_dag.py`

This DAG:

1. Starts the Dataproc cluster.
2. Runs Hospital A ingestion.
3. Runs Hospital B ingestion.
4. Runs claims ingestion.
5. Runs CPT ingestion.
6. Stops the Dataproc cluster.

Dependency chain:

```text
Start Dataproc
      |
Hospital A
      |
Hospital B
      |
Claims
      |
CPT
      |
Stop Dataproc
```

Starting and stopping the cluster around the workload helps avoid keeping a dedicated cluster running when no processing is occurring.

### `bq_dag.py`

This DAG executes the BigQuery transformation layers sequentially:

```text
Bronze
  |
  v
Silver
  |
  v
Gold
```

---

## 14. Cloud Build Deployment

`cloudbuild.yaml` provides a lightweight deployment mechanism.

The build process:

1. Installs the Python dependency from `utils/requirements.txt`.
2. Runs `utils/add_dags_to_composer.py`.
3. Uploads the Airflow DAGs to the Composer bucket.
4. Uploads the `data/` directory to the Composer bucket.

Configured substitution variables include:

```text
_DAGS_DIRECTORY
_DAGS_BUCKET
_DATA_DIRECTORY
```

This allows the deployment target to be changed without modifying the build logic.

---

## 15. Repository Structure

```text
gcp-healthcare-project-main/
│
├── ProjectNotes.md
├── cloudbuild.yaml
│
├── data/
│   │
│   ├── BQ/
│   │   ├── bronze.sql
│   │   ├── silver.sql
│   │   ├── gold.sql
│   │   └── assignment.sql
│   │
│   ├── EMR/
│   │   ├── hospital-a/
│   │   │   ├── ddl.sql
│   │   │   ├── departments.csv
│   │   │   ├── encounters.csv
│   │   │   ├── patients.csv
│   │   │   ├── providers.csv
│   │   │   └── transactions.csv
│   │   │
│   │   └── hospital-b/
│   │       ├── ddl.sql
│   │       ├── departments.csv
│   │       ├── encounters.csv
│   │       ├── patients.csv
│   │       ├── providers.csv
│   │       └── transactions.csv
│   │
│   ├── INGESTION/
│   │   ├── hospitalA_mysqlToLanding.py
│   │   ├── hospitalB_mysqlToLanding.py
│   │   ├── claims.py
│   │   ├── cpt_codes.py
│   │   ├── icd_codes.py
│   │   └── npi_codes.py
│   │
│   ├── configs/
│   │   ├── audit_table_ddl.sql
│   │   └── load_config.csv
│   │
│   ├── claims/
│   └── cptcodes/
│
├── utils/
│   ├── add_dags_to_composer.py
│   └── requirements.txt
│
└── workflows/
    ├── parent_dag.py
    ├── pyspark_dag.py
    └── bq_dag.py
```

---

## 16. Prerequisites

Before deploying, make sure the following are available:

- Google Cloud project
- Billing-enabled GCP project
- Cloud Storage bucket
- BigQuery datasets
- Cloud SQL/MySQL instances for the EMR sources
- Dataproc environment
- Cloud Composer environment
- Cloud Build enabled
- Required IAM permissions
- Python 3.x
- PySpark / Dataproc runtime compatible with the project
- BigQuery and GCS APIs enabled

Recommended GCP APIs:

```text
Cloud Storage
BigQuery
Dataproc
Cloud Composer
Cloud Build
Cloud SQL Admin
Secret Manager
IAM
```

---

## 17. Deployment Steps

### Step 1: Create the GCP Resources

Create:

```text
Cloud Storage bucket
Cloud SQL / MySQL databases
BigQuery datasets
Dataproc environment/cluster configuration
Cloud Composer environment
```

Example BigQuery datasets:

```text
bronze_dataset
silver_dataset
gold_dataset
temp_dataset
```

### Step 2: Load the Source Databases

Create the Hospital A and Hospital B tables using:

```text
data/EMR/hospital-a/ddl.sql
data/EMR/hospital-b/ddl.sql
```

Load the sample data into the corresponding Cloud SQL/MySQL databases.

### Step 3: Configure GCS

Upload the configuration file:

```text
data/configs/load_config.csv
```

to the GCS configuration path expected by the ingestion scripts.

### Step 4: Create BigQuery Audit Tables

Run:

```text
data/configs/audit_table_ddl.sql
```

### Step 5: Configure the Code

Replace environment-specific values such as:

```text
GCP project ID
GCS bucket
Composer bucket
Cloud SQL host
database names
database credentials
Dataproc cluster name
Dataproc region
```

Do not commit production credentials.

### Step 6: Deploy with Cloud Build

Run the Cloud Build pipeline after configuring the substitution variables in `cloudbuild.yaml`.

The build uploads:

```text
workflows/ -> Composer dags/
data/      -> Composer data/
```

### Step 7: Trigger Airflow

From Cloud Composer/Airflow:

```text
parent_dag
    |
    +--> pyspark_dag
    |
    +--> bigquery_dag
```

After successful execution, the BigQuery Gold layer will contain the analytics outputs.

---

## 18. Example Analytics Use Cases

The Gold layer can support questions such as:

### Revenue

- What is the total billed amount?
- How much has been paid?
- What is the outstanding balance?
- Which departments generate the most revenue?

### Provider Performance

- Which providers have the highest encounter volume?
- What is each provider's billed and paid amount?
- What is the claim approval rate by provider?

### Department Performance

- Which departments have the most encounters?
- Which departments generate the highest revenue?
- What is the average payment per transaction?

### Claims / Payors

- Which payors have the highest approval rate?
- How many claims are denied or pending?
- Which payors have the highest outstanding amount?

### Patient History

- What encounters has a patient had?
- What transactions are associated with the patient?
- What claims and payments are associated with those transactions?

---

## 19. Key Data Engineering Concepts Demonstrated

This project demonstrates several concepts relevant to a production GCP Data Engineer role:

- Multi-source data ingestion
- Cloud SQL / MySQL ingestion
- JDBC-based extraction
- Full vs incremental loading
- Watermark-based ingestion
- GCS data lake design
- File archival
- PySpark processing
- Dataproc orchestration
- BigQuery external tables
- BigQuery ELT
- Bronze/Silver/Gold architecture
- Data quality validation
- Data quarantine
- SCD Type 2
- MERGE-based transformations
- Audit logging
- Pipeline logging
- Airflow DAG orchestration
- Parent/child DAG dependencies
- Cloud Build deployment
- API-based data ingestion
- Healthcare RCM analytics

---

## 20. Production Improvements

For a production deployment, the following improvements are recommended.

### Secrets Management

Move:

- Database usernames
- Database passwords
- API client IDs
- API client secrets

to **Google Secret Manager**.

### Configuration Management

Move hardcoded values such as:

```text
project IDs
bucket names
regions
Composer bucket
Dataproc cluster
```

to environment variables, Airflow Variables, Secret Manager, or deployment configuration.

### Avoid Credentials in Source Control

Credentials and API secrets should never be committed to Git.

If a credential has already been exposed in a repository, rotate/revoke it before deploying.

### Improve Incremental Watermark Logic

The current implementation uses the audit load timestamp as the watermark reference. A stronger production design would store the actual source-system watermark value for each table and load.

### Improve Large-Data Handling

The EMR ingestion currently converts Spark DataFrames to Pandas before writing JSON. For large healthcare datasets, this can create driver-memory pressure.

A production implementation should write directly from Spark to GCS using a distributed format such as:

```text
Parquet
Avro
JSON Lines
```

depending on downstream requirements.

### Improve Airflow Reliability

Consider adding:

- Explicit Airflow connections
- Environment-specific configuration
- Sensors where appropriate
- Better failure callbacks
- Centralized alerting
- Task-level logging
- Data quality checks
- SLA monitoring

### Security

For healthcare workloads, apply appropriate controls such as:

- Least-privilege IAM
- Private connectivity for Cloud SQL
- Encryption and CMEK where required
- VPC Service Controls where appropriate
- Secret Manager
- Audit logging
- Data classification
- Sensitive Data Protection / DLP
- Appropriate retention policies

---

## 21. Technology Stack

```text
Cloud Platform:
Google Cloud Platform (GCP)

Storage:
Google Cloud Storage

Databases:
Cloud SQL / MySQL

Processing:
Apache Spark
PySpark
Google Cloud Dataproc

Data Warehouse:
Google BigQuery

Orchestration:
Apache Airflow
Cloud Composer

CI/CD:
Cloud Build

Programming:
Python
SQL
PySpark

Data Formats:
CSV
JSON
Parquet

APIs:
CMS NPI Registry API
WHO ICD API
```

---

## 22. Resume / Portfolio Summary

This project can be described on a resume as:

> **Healthcare Revenue Cycle Management ETL Pipeline — GCP**  
> Designed and implemented an end-to-end healthcare data pipeline on GCP integrating Cloud SQL/MySQL EMR data, insurance claims, CPT/ICD/NPI reference data, GCS, Dataproc/PySpark, BigQuery, Cloud Composer (Airflow), and Cloud Build. Implemented full and incremental ingestion with watermark-based processing, GCS archival, audit logging, data-quality quarantine, SCD Type 2 transformations, and Bronze/Silver/Gold BigQuery layers for provider, department, patient, financial, and claims analytics.

---

## 23. Project Outcome

The final architecture provides a complete path from operational healthcare data to analytics-ready information:

```text
Operational Sources
       ↓
Cloud SQL + Files + APIs
       ↓
Dataproc / PySpark
       ↓
GCS Landing + Archive
       ↓
BigQuery Bronze
       ↓
Data Quality + SCD Type 2
       ↓
BigQuery Silver
       ↓
Business Transformations
       ↓
BigQuery Gold
       ↓
Healthcare RCM Analytics
```

The project demonstrates practical GCP data-engineering patterns rather than only isolated service usage, with ingestion, processing, orchestration, warehouse modeling, operational logging, and deployment automation working together as a single pipeline.
