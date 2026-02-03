# TfL Bus Performance Lakehouse 🚍

A **business-driven, production-style data engineering project** that demonstrates how raw public transport data can be transformed into **decision-ready insights** using modern cloud analytics.

This project is intentionally designed to showcase my ability to:
- Translate **business problems into data solutions**
- Design **scalable, fault-tolerant architectures**
- Build **end-to-end pipelines** with analytics impact
- Deliver **clear KPIs** that support operational and strategic decisions

---

## Problem Statement

Transport authorities rely on accurate and timely insights to ensure reliable, equitable, and efficient public transport services. However, TfL bus data presents several challenges:

- Operational data is spread across **multiple API endpoints**
- Raw data is **unstructured and inconsistent**
- There is limited visibility into:
  - Service reliability and SLA compliance
  - Disruption frequency and impact
  - Borough-level service equity

Without a unified data platform, stakeholders struggle to **monitor performance trends**, **identify problem areas**, and **make evidence-based decisions**.

---

## Solution Overview

I designed and implemented a **cloud-native Lakehouse analytics platform on Azure** that converts raw TfL bus data into **trusted business KPIs**.

The solution:
- Automates ingestion of real-time and reference data
- Applies a **Medallion Architecture (Bronze → Silver → Gold)** to ensure data quality
- Produces analytics-ready datasets optimised for reporting
- Enables stakeholders to monitor performance through dashboards

This mirrors how data platforms are built in **real-world enterprise environments**.

---

## Architecture Overview

### High-Level Architecture

> **Figure 1 – End-to-End TfL Bus Analytics Architecture**  
> Shows how raw TfL API data is orchestrated through Azure Data Factory, stored in Azure Data Lake, transformed in Databricks, and exposed to analytics tools for decision-making.
<img width="1536" height="1024" alt="Tfl Architecture" src="https://github.com/user-attachments/assets/c10a1ec7-b336-4069-8442-b049a6b1d4c1" />


### Medallion Data Design

> **Figure 2 – Medallion Architecture for Data Quality and Trust**  
> Separates raw ingestion, transformation, and business logic to ensure scalability, auditability, and reliable analytics.


- **Bronze** – Raw, immutable API data  
- **Silver** – Cleaned, enriched, validated datasets  
- **Gold** – Business KPIs and analytics-ready views  

---

## Technical Features

### Azure Data Factory (ADF)
- Parameterised pipelines for reusable ingestion
- Pagination support for endpoints with page-based responses
- `Lookup` + `ForEach` orchestration for dynamic line/stop iteration
- Controlled concurrency to reduce throttling risk
- Centralised error-handling pipeline with failure routing

### Azure Data Lake Storage Gen2 (ADLS)
- Medallion-aligned storage structure (Landing/Bronze/Silver/Gold)
- Partitioned landing paths by date/time for traceability

### Databricks + Delta Lake
- Autoloader (`cloudFiles`) from Landing → Bronze
- Delta tables for ACID guarantees and performant analytics
- Idempotent transformations and incremental upserts using `MERGE INTO`

### Analytics Consumption
- Gold KPI tables/views optimised for BI queries
- Dashboard-ready datasets served via Databricks SQL / Power BI

---

## Data Sources & Pipeline Workflow

### Data Sources (TfL APIs)
- `/line_status` – service status and disruption context
- `/arrivals` – real-time arrivals/events
- `/stop_points` – stop metadata and location context
- `/london_boroughs` – borough reference data

### End-to-End Workflow
1. **ADF** extracts data from TfL APIs (parameterised + metadata-driven)
2. Data lands as raw JSON in **ADLS Landing**
3. **Autoloader** ingests Landing files into **Bronze Delta tables**
4. Databricks transforms Bronze → **Silver** (clean, standardised, enriched)
5. Databricks transforms Silver → **Gold** (KPI views/tables)
6. BI layer consumes Gold for dashboards and reporting

---

## Validation and Testing

- Schema validation between layers (Bronze → Silver → Gold)
- Row-count reconciliation checks per run
- Null/anomaly checks on key columns (timestamps, ids, metrics)
- Business rule validation for SLA classification (e.g., `meets_SLA`)
- Spot checks using Databricks SQL query results

---

## CI/CD & Deployment Implementation

This repository is structured for production-style delivery:

- Version-controlled notebooks and SQL models
- Parameter-driven design to support multi-environment deployments
- Databricks Jobs for orchestration and scheduling (hourly runs)
- ADF pipeline artefacts can be deployed via ARM/Bicep/Terraform or CI workflows

---

## How I Solved the Business Problem (Technical Deep Dive)

This section explains how I designed the platform to handle real-world constraints like **API throttling**, **partial failures**, **schema drift**, and the need for **trusted business metrics**.

---

### 1) ADF Ingestion – Parameterised, Metadata-Driven Design

Instead of building separate pipelines per endpoint, I implemented a **single reusable ingestion framework** in **Azure Data Factory (ADF)** driven by metadata.

**What I parameterised**
- `baseUrl` / `endpoint`
- `target_path` (ADLS landing folder)
- `pagination` controls (page number, termination condition)
- dynamic iteration over `lineIds` and `naptanIds`
- `run_id` and partition date/time

**How metadata drives ingestion**
I maintain a small configuration (JSON, SQL, or a control table) that defines each ingestion “unit”:

- endpoint name + URL template
- whether it requires `lineId` iteration
- throttle/retry settings
- landing path

This enables adding new sources without duplicating pipelines.

> **Screenshot – ADF Orchestration with Lookup + ForEach**  
> `Lookup_LineIds` feeds `ForEach_BusArrival` and `ForEach_StopPoint`, with failure routing into dedicated error handler pipelines.

<img width="1727" height="780" alt="Screenshot 2026-01-19 at 14 39 28" src="https://github.com/user-attachments/assets/69973fcc-2209-42cb-8c7d-f7f1884d7d3f" />


---

### 2) Handling API Pushback – Retries, Throttling & Concurrency Controls

TfL APIs can return rate-limit and intermittent failures. I designed ingestion to **recover automatically**.

- **Retries & backoff**: configured activity retries for transient errors (timeouts/5xx)
- **Throttling**: controlled `ForEach` concurrency to limit parallel calls
- **Pagination**: used parameters like `MaxPage` to drive looping safely

**Outcome:** stable ingestion runs without manual intervention, reducing data gaps that would corrupt KPIs.

---

### 3) Error Handling Pipeline – Centralised Failure Management

To prevent “silent failures,” I created a central error-handling pattern:

- Each critical activity has an **On Failure** dependency
- Failures call an `Execute Pipeline` step (e.g., `PL_ErrorHandler`)
- The error handler captures context (endpoint/lineId/activity/error/run id) for troubleshooting and repeatability

This allows partial pipeline success while still surfacing actionable failures.

---

### 4) Bronze Layer – Autoloader from Landing → Bronze Delta

Once raw API JSON lands in ADLS, I use **Databricks Autoloader** to ingest into **Bronze Delta tables**.

**Why Autoloader**
- Incremental file discovery
- **Schema evolution** as new fields appear
- Checkpointing for reliable ingestion

**Key design choices**
- `cloudFiles` JSON ingestion
- `schemaLocation` for schema persistence
- `schemaEvolutionMode = addNewColumns`
- `maxFilesPerTrigger = 1` to control ingestion pressure
- operational columns: `_ingest_time`, `_source_file`

```python
# Read raw stream data into a dataframe using Autoloader

df_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .option("maxFilesPerTrigger", 1)
        .load(file_path)
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
)

# Write streaming data to delta table
stream_writer = (
    df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint)
        .queryName(target_table)
)

# Trigger streaming write either once or continuously
if once:
    stream_writer.trigger(availableNow=True).toTable(f"{schema_bronze}.{target_table}")
else:
    stream_writer.trigger(processingTime=processing_time).toTable(f"{schema_bronze}.{target_table}")
```



### 5) Silver Layer – Cleaning, Transformation, Enrichment & MERGE Upserts

The Silver layer is where raw JSON becomes **trusted, analytics-ready datasets**. My goal here was to ensure that downstream KPIs are built on data that is:

- **Clean** (consistent types, no broken timestamps)
- **Standardised** (stable schemas across runs)
- **Enriched** (joined to reference data for business meaning)
- **Incremental & correct** (idempotent updates using upserts)

This is critical because the business questions (reliability, disruption impact, equity) require **stable and accurate joins** across routes, stops, and boroughs — not raw event payloads.

---

#### 5.1 Key Transformation Steps (Bronze ➜ Silver)

**A. Schema Normalisation**
- Flatten nested JSON fields into tabular columns
- Cast key fields to correct types:
  - timestamps (`service_date`, `expected_arrival`, `timestamp`)
  - ids (`line_id`, `stop_id`, `naptan_id`)
  - numerics (wait times, headway/interval metrics)
- Standardise naming conventions (`snake_case`) for consistency

**B. Data Quality Rules**
- Filter out invalid events:
  - missing stop id or line id
  - timestamps outside expected range (future drift)
- Remove duplicates using stable keys such as:
  - `(line_id, naptan_id, vehicle_id, expected_arrival)`
- Add audit/lineage columns:
  - `_source_file`
  - `_ingest_time`

**C. Business Semantics**
- Convert the raw arrivals stream into an **events table** with:
  - arrival time dimensions
  - wait time / headway fields
  - disruption flags

---

#### 5.2 Data Enrichment (Derived Tables)

Silver is where I derived higher-value datasets by enriching operational events with reference context.

**Enrichment performed**
- **Arrivals ➜ Stops**  
  Join arrivals events to stop metadata (`stop_points`) to attach:
  - stop name
  - latitude/longitude
  - stop type
  - parent station/group relationships (when applicable)

- **Stops ➜ Boroughs**  
  Map stop points to borough reference (`london_boroughs`) to enable:
  - borough-level disruption aggregation
  - borough service equity metrics

- **Line Status ➜ Disruption Indicators**  
  Derive disruption attributes such as:
  - disruption category
  - severity level
  - disruption active flag
  - affected route indicators

**Why enrichment matters**
Without borough/stop mapping, you cannot answer:
- *Which boroughs are most affected?*
- *Is service disruption equitable across London?*

---

#### 5.3 Incremental Processing With MERGE INTO (Upserts)

Because the pipeline runs **every hour**, I designed Silver tables to support incremental updates safely.

**Why not append-only?**
- API responses can repeat the same events
- Some events arrive late
- Corrections can appear across hourly runs
- Append-only would cause duplicates and inflate KPIs

So I implemented **idempotent incremental upserts** using `MERGE INTO`.

---

#### 5.4 `foreachBatch` Upsert Pattern

I used a reusable helper function to execute a merge query per micro-batch during streaming writes:

```python
# Helper function to perform upsert using a merge query in foreachBatch
def upserter(df_micro_batch, batch_id, merge_query, temp_view):
    df_micro_batch.createOrReplaceTempView(temp_view)
    df_micro_batch._jdf.sparkSession().sql(merge_query)
    print(f"Batch {batch_id} for {temp_view} processed.")
```

### 6) Gold Layer – KPI Views for Business Questions (Summary)

In the Gold layer, I transformed enriched Silver datasets into **business-ready KPI views** that directly answer stakeholder questions about reliability, disruption impact, and equity. I implemented Gold as modular, reusable functions to keep KPI logic consistent and maintainable across hourly runs:

- `create_line_reliability_kpi()` → identifies lines meeting/breaching SLA reliability
- `create_line_regularity_kpi()` → measures arrival consistency / service stability
- `create_line_disruption_impact_kpi()` → quantifies disruption frequency and impact per line
- `create_borough_service_equity_kpi()` → highlights which boroughs are disproportionately affected

Gold outputs are optimised for **fast BI querying** and are used as the single source of truth for dashboards and reporting.

---

### 7) Databricks Jobs – Hourly Scheduling & Operationalisation (Summary)

I operationalised the entire pipeline using **Databricks Jobs**, scheduled to run **every 1 hour** to keep insights fresh. The hourly job orchestrates:

1. **Landing → Bronze** ingestion using Autoloader (schema evolution + checkpoints)
2. **Bronze → Silver** cleaning and enrichment with incremental **MERGE INTO upserts**
3. **Silver → Gold** KPI refresh by executing the four KPI functions above
4. **Validation + logging** to ensure data quality and consistent KPI outputs

This setup ensures the pipeline is **production-style**, resilient to reruns, and delivers near real-time KPIs for business decision-making.

<img width="1007" height="447" alt="Screenshot 2026-02-03 at 15 49 47" src="https://github.com/user-attachments/assets/7aef19a5-3313-4e82-9077-368ad540f4e3" />

<img width="1120" height="686" alt="Screenshot 2026-02-03 at 15 49 06" src="https://github.com/user-attachments/assets/db2d3b46-0758-4081-8ac8-2aef6b779e0a" />

---

## Business Impact & Usage

This platform turns raw transport data into **business outcomes** and operational visibility.

### Key Outcomes Delivered
- 📊 **Near real-time monitoring** of bus reliability and disruption patterns
- ⚠️ Faster identification of **problem lines** and disruption hotspots
- ⚖️ Evidence-based analysis of **service equity** across boroughs
- 🏛️ KPI-ready datasets supporting planning, reporting, and performance review

---

### Example Business Questions Answered
- Which bus lines consistently breach reliability expectations?
- Which boroughs are most affected by service disruptions?
- How does service reliability trend over time?
- Are service disruptions disproportionately affecting some boroughs?

---

### Dashboard Output (Stakeholder View)

> **Figure – KPI Dashboard**
> Shows total lines processed, disruption counts, borough rankings, SLA status, and performance trends.
<img width="1528" height="861" alt="Screenshot 2026-02-03 at 12 37 31" src="https://github.com/user-attachments/assets/5248c9b5-6192-4208-b0a8-ccd2a5e17629" />



---

✅ In summary, this project demonstrates my ability to build an end-to-end analytics platform that is:
- technically resilient (retries, throttling, checkpointing, upserts)
- scalable (metadata-driven ingestion and medallion design)
- business-focused (KPIs aligned to stakeholder decision-making)

