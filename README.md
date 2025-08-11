# Data Pipeline Project Using PySpark and Airflow: Startup Ecosystem
This document outlines an end-to-end data pipeline designed to integrate, process, and analyze data from the startup ecosystem across various sources. The project enables in-depth analysis of investment trends, company performance, and key player networks. It builds upon the  [Data Pipeline with Pyspark](https://github.com/oscar-sinaga/data_pipeline_pyspark) project, with modifications to incorporate Apache **Apache Airflow** for orchestration.

## Table of Contents
- [Data Pipeline Project Using PySpark and Airflow: Startup Ecosystem](#data-pipeline-project-using-pyspark-and-airflow-startup-ecosystem)
  - [Table of Contents](#table-of-contents)
  - [Requirements Gathering \& Solution](#requirements-gathering--solution)
    - [Problem Background](#problem-background)
      - [1. Difficulty in Accurately Gauging Growth Momentum](#1-difficulty-in-accurately-gauging-growth-momentum)
      - [2. Fragmented Analysis of Exit Strategies](#2-fragmented-analysis-of-exit-strategies)
      - [3. Limitations in Mapping the Human Capital Network](#3-limitations-in-mapping-the-human-capital-network)
    - [Proposed Solution](#proposed-solution)
    - [Data Profiling](#data-profiling)
- [Data Quality Assessment and Handling Strategy](#data-quality-assessment-and-handling-strategy)
  - [1. Low Completeness in Key Metrics](#1-low-completeness-in-key-metrics)
    - [Impact on Funding \& Exit Analysis](#impact-on-funding--exit-analysis)
    - [Impact on Network \& Career Mapping](#impact-on-network--career-mapping)
    - [Impact on Geographical Analysis](#impact-on-geographical-analysis)
  - [2. Issues with Data Integrity and Formatting](#2-issues-with-data-integrity-and-formatting)
    - [Date Data Types](#date-data-types)
    - [Pipeline Architecture Design](#pipeline-architecture-design)
  - [Target Database Design (Data Warehouse)](#target-database-design-data-warehouse)
    - [Business Process 1: Evaluating Startup Funding Journeys and Growth](#business-process-1-evaluating-startup-funding-journeys-and-growth)
      - [Fact Tables](#fact-tables)
      - [Dimension Tables](#dimension-tables)
    - [Business Process 2: Analyzing Exit Strategies and Startup Market Performance](#business-process-2-analyzing-exit-strategies-and-startup-market-performance)
      - [Fact Tables](#fact-tables-1)
      - [Dimension Tables](#dimension-tables-1)
    - [Business Process 3: Mapping the Ecosystem and Key Player Networks](#business-process-3-mapping-the-ecosystem-and-key-player-networks)
      - [Fact Tables](#fact-tables-2)
      - [Dimension Tables](#dimension-tables-2)
  - [Final Data Warehouse Design Summary](#final-data-warehouse-design-summary)
    - [Dimension Tables – Providing Context (Who, What, Where, When)](#dimension-tables--providing-context-who-what-where-when)
    - [Fact Tables – Capturing Business Events \& Metrics](#fact-tables--capturing-business-events--metrics)
  - [ETL Workflow Design](#etl-workflow-design)
    - [Staging Layer](#staging-layer)
    - [Warehouse Layer](#warehouse-layer)
  - [Technologies Used](#technologies-used)
  - [How to Run the Pipeline](#how-to-run-the-pipeline)


## Requirements Gathering & Solution

### Problem Background
An investment consulting firm, **"VenturePulse"** serves a diverse clientele ranging from startups to financial institutions. In its mission, VenturePulse faces significant challenges in comprehensively integrating and analyzing data from disparate sources. Details about these data sources can be found in [dataset-doc.md](dataset-doc.md). These data access and complexity issues lead to several business problems:


#### 1. Difficulty in Accurately Gauging Growth Momentum

- **Situation**: Funding data (`funding_rounds`, `investments`, `funds`) and achievement data (`milestones`) are available from various sources but are not explicitly linked in an analytical model.
- **Problem:**: It is difficult to directly assess the impact of funding on a startup's growth. Questions like, "Did the Series B funding round drive a major product launch?" cannot be answered directly due to the lack of a clear link between funding timelines, capital sources, and business achievements.


**Key Tables:**
- `funding_rounds`, `investments`, `funds`, `milestones`

---

#### 2. Fragmented Analysis of Exit Strategies

- **Situation:** Data on acquisitions (`acquisitions`) and IPOs (`ipos`) exist in the database but lack the detailed company or temporal attributes needed for longitudinal and sector-based analysis.

- **Problem:** Without a structured data model to compare exit activities and values, it is challenging to perform comparative analyses across industries, timeframes, or exit strategy types. Questions like, "What was the average IPO valuation in the fintech sector over the last 5 years?" or "Which corporation acquires startups most frequently?" cannot be answered efficiently.

**Key Tables:**
- `acquisitions`, `ipos`,  `ipos`

---

#### 3. Limitations in Mapping the Human Capital Network

- **Kondisi:** `relationships` data linking individuals to companies is available, as is company achievement data (`milestones`). However, these are not integrated into a single, centralized source that would allow for easy career tracking and innovation mapping connected to other data points.
- **Problem:** It is difficult to trace an individual's contributions to growth and innovation across different companies. Visualizing networks or analyzing the impact of human capital on startup performance is incomplete due to the limited connections between individuals, their roles, and tangible business outcomes.


**Key Tables:**
- `relationships`, `milestones`, `people`, `company`

---

### Proposed Solution

To address these challenges, we will build a **Centralized Data Pipeline** that automates the collection, cleaning, transformation, and storage of data from various sources into a single **Data Warehouse**.
The goal is to provide reliable, integrated, and analysis-ready data for strategic decision-making with minimal manual intervention.


### Data Profiling
The data profiling process helps identify specific data quality issues. These findings are foundational for defining the transformation steps in the ETL process, as they directly impact the accuracy and completeness of the subsequent business analysis.

# Data Quality Assessment and Handling Strategy

## 1. Low Completeness in Key Metrics

Many columns crucial for business analysis have a very high percentage of missing values, directly hindering the defined business processes.

### Impact on Funding & Exit Analysis
- In the `funding_rounds` table, valuation data is highly incomplete:
  - **48.1%** missing values in `pre_money_currency_code`
  - **41.63%** missing values in `post_money_currency_code`
- In the `acquisitions` table:
  - **81.67%** of `term_code` (acquisition type: cash/stock) data is missing
- **Implication:**  
  Without `pre_money_currency_code` and `post_money_currency_code`, any analysis involving investment amounts is impossible. It becomes unfeasible to evaluate funding performance or comprehensively analyze exit strategies if the amounts depend on their currency. The ETL process must implement a strategy to handle these missing values before loading data into `fact_investment_round_participation` and `fact_acquisitions`.

**ETL Handling Plan:**
- In `funding_rounds`:
  - Drop `post_money_currency_code` and `pre_money_currency_code`
  - Reason: `pre_money_valuation_usd` and `post_money_valuation_usd` are populated and already in USD
- In `acquisitions`:
  - Fill missing `term_code` with `Unknown`

---

### Impact on Network & Career Mapping
- In the `relationships` table:
  - **53.48%** missing in `start_at`
  - **85.71%** missing in `end_at`
- **Implication:**  
  This fundamentally undermines the ability to "Map the Human Capital Network." Analysis of career duration or an individual's active period at a company becomes inaccurate. Transformations for `fact_relationship` must handle these null dates.

**ETL Handling Plan:**
- Populate missing `start_at` and `end_at` with a far-future date (`2100-01-01`) to indicate the original date was empty, allowing easy filtering during analysis.

---

### Impact on Geographical Analysis
- In the `company` table:
  - **42.36%** missing in `state_code`
  - **4.59%** missing in `city`
- **Implication:**  
  Location-based analysis becomes less reliable.

---

## 2. Issues with Data Integrity and Formatting

### Date Data Types
- Most date columns across various tables (`funding_rounds.funded_at`, `acquisitions.acquired_at`, `relationships.start_at`) are stored as `object` (string) types, not `datetime`.
- **Implication:**  
  This blocks all forms of time-based analysis (trends, durations, etc.).

**ETL Handling Plan:**
- Convert all date columns to `YYYYMMDD` integer format
- Use `date_id` as a foreign key to the `dim_date` table

### Pipeline Architecture Design

The pipeline consists of several key components:

- **Layers:**
  - **Data Sources:** PostgreSQL, CSV/JSON files, and external APIs.
  - **Staging Layer:** Stores raw data in a PostgreSQL schema `staging` for further transformation.
  - **Warehouse Layer:** Stores transformed, structured data in a `warehouse` schema based on a Star Schema model.

- **Logging & Monitoring:**
  - Information for each ETL process will be recorded in a dedicated PostgreSQL log database.

- **Orchestration and Scheduling:**
  - All ETL processes are executed and scheduled by the Apache Airflow orchestration tool.

- **Temporary Data and Validation:**
  - Temporary data between the extract and load steps is stored in Minio data lake buckets as **CSV** and **Parquet** files.
  - Invalid data is also stored in the Minio data lake for inspection.

- **Error Alerting:**
  - Any errors occurring in a DAG run will trigger a notification to **Slack**.

![Pipeline Design](picture/data_pipeline_workflow.drawio.png)


## Target Database Design (Data Warehouse)

The data warehouse structure is designed using the **Kimball methodology** with a **Star Schema** approach.  
This model centers on **fact tables** (measurable business events) surrounded by **dimension tables** that provide context ("who," "what," "when," "where").

---

### Business Process 1: Evaluating Startup Funding Journeys and Growth

**Focus:** Measuring capital flow, growth momentum, and the startup funding network.

#### Fact Tables

- **`fact_investment_round_participation`**
  - **Grain:** One row represents a single investor's unique participation in one funding round for a specific company, including investment value (if available), lead investor status, and investment stage.
  - **Role:** Crucial for funding analysis, enabling co-investor network analysis and answering questions like “Who invests alongside whom?”.

- **`fact_funds`**
  - **Grain:** One row represents a single fund raised by an investor at a specific time.
  - **Role:** Records details of funds (e.g., from VCs/PE firms) used to invest in startups. Useful for:
    - Analyzing an investor's funding capacity.
    - Tracking fundraising history and growth.

- **`fact_milestones`**
  - **Grain:** One row represents a unique milestone event (e.g., product launch, acquisition, partnership, fundraising) at a company on a specific date.
  - **Role:** Adds narrative and temporal context about significant achievements not directly shown in funding numbers.

#### Dimension Tables

- **`dim_company`**
  - **Grain:** One row per unique company.
  - **Role:** Provides descriptive and geographical company information for cross-domain analysis.

- **`dim_date`**
  - **Grain:** One row per calendar day.
  - **Role:** Provides a time framework for analyzing funding trends.

---

### Business Process 2: Analyzing Exit Strategies and Startup Market Performance

**Focus:** Analyzing peak events like acquisitions and IPOs, evaluating value and timing of exits as part of investment strategy.

#### Fact Tables

- **`fact_acquisition`**
  - **Grain:** One row represents a unique acquisition event linking the acquirer, the acquired company, and the acquisition date.
  - **Role:** Tracks acquisition volume and value as market liquidity indicators; supports analysis by time, region, industry, or valuation.

- **`fact_ipos`**
  - **Grain:** One row represents a unique IPO event, linked to the company and IPO date.
  - **Role:**
    - Measures exit via public markets.
    - Provides valuation and capital raised at IPO.
    - Enables comparison of IPO vs acquisition strategies.
    - Helps measure investor returns when combined with funding data.

#### Dimension Tables

- **`dim_company`**
  - **Grain:** One row per unique company.
  - **Role:** Reference for companies that acquire, are acquired, or go public; provides location and geographic metadata.

- **`dim_date`**
  - **Grain:** One row per unique calendar day.
  - **Role:** Time framework for analyzing exit trends, supports aggregation (weekly, monthly, quarterly, yearly) and seasonal trend analysis.

---

### Business Process 3: Mapping the Ecosystem and Key Player Networks

**Focus:** Mapping individual contributions and relationships in the startup ecosystem.

#### Fact Tables

- **`fact_relationship`**
  - **Grain:** One row per specific working relationship between an individual and a company.
  - **Role:** Primary source for mapping human capital; essential for career tracking and network analysis.

- **`fact_milestones`**
  - **Grain:** One row represents a specific milestone event associated with a company.
  - **Role:** Maps innovation outputs and can identify high-innovation regions.

#### Dimension Tables

- **`dim_people`**
  - **Grain:** One row per unique individual.
  - **Role:** Main subject for career mapping.

- **`dim_company`**
  - **Grain:** One row per company.
  - **Role:** Provides organizational and geographical context for professional relationships.

- **`dim_date`**
  - **Grain:** One row per calendar day.
  - **Role:** Temporal framework for professional activities.


## Final Data Warehouse Design Summary

The structure follows the **Kimball Method** using a **Star Schema**,  
where **dimension tables** store context (*who, what, where, when*)  
and **fact tables** store measurable business events.

---

### Dimension Tables – Providing Context (Who, What, Where, When)

| Table Name    | Role                                                                                                                                  | Grain                                   |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `dim_date`    | A standard calendar to accommodate trend analysis and time-based aggregations (daily, weekly, monthly, quarterly, yearly, seasonal). | One row per unique calendar day         |
| `dim_company` | A unique representation of each company, including descriptive (name, industry) and geographical (country, city, region) information.| One row per unique company              |
| `dim_people`  | A unique representation of each individual in the startup ecosystem (founders, executives, investors, key employees) with demographic & career attributes. | One row per unique individual           |

---

### Fact Tables – Capturing Business Events & Metrics

| Table Name                          | Role                                                                                                                                                                                                                 | Grain                                                                |
|-------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `fact_investment_round_participation` | Records an investor's unique participation in a company's funding round, including investment amount, lead status, and funding stage.                                                                              | One row per investor–funding round–company combination               |
| `fact_funds`                        | Records funds raised by investors (VCs, PEs) at a specific time, used to measure funding capacity and history.                                                                                                      | One row per unique fund                                               |
| `fact_acquisitions`                 | Records startup acquisition events (value, date, acquirer, and acquiree), serving as an indicator of market liquidity and exit strategies.                                                                          | One row per unique acquisition event                                 |
| `fact_ipos`                         | Records IPO events (stock value, funds raised, date), used to measure the public market exit path and compare IPO vs. acquisition strategies.                                                                       | One row per unique IPO event                                          |
| `fact_milestones`                   | **Factless Fact Table** – Records significant company achievements or activities (e.g., product launches, partnerships) as indicators of innovation and progress.                                                   | One row per unique milestone                                          |
| `fact_relationship`                 | **Factless Fact Table** – Records unique working relationships between individuals and companies, for human capital mapping and professional network analysis.                                                      | One row per unique working relationship                               |

---

A detailed source-to-target mapping is available in the following document:  
📄 [source_to_target_mapping.md](source_to_target_mapping.md)


## ETL Workflow Design

### Staging Layer
1. **Extract:** PySpark pulls data from various sources (APIs, source databases, and spreadsheets). The extracted data is temporarily stored in the `extracted-data` bucket in **Minio**.
2. **Load:** Data collected from multiple sources in Minio by PySpark is then stored in the **PostgreSQL Staging Database**.

### Warehouse Layer
1. **Extract and Transform:**  
   Raw data stored in the **Staging Database** is extracted and transformed by PySpark.  
   The transformation process includes:
   - Data cleaning (null handling, duplicates removal)
   - Format standardization
   - Column enrichment
   - Table integration based on `object_id`
   
   After transformation, the data is temporarily stored in the `transformed-data` bucket in **Minio**.

2. **Validation:**  
   The transformed data is validated, and the validation report is stored in **Minio** as a `.json` file.

3. **Load:**  
   Once validated, the transformed data stored in Minio is loaded into the **PostgreSQL Warehouse Database**.

Every ETL process for each table is logged in the **Log Database**. The workflow for each layer is orchestrated by **Airflow**.  
If an error occurs during a process, the error information is sent to a **Slack** account.  
The final transformed data stored in the warehouse can then be visualized as dashboards in **Metabase**.

---

## Technologies Used

- **Language:** Python
- **Processing Engine:** Apache Spark (via PySpark)
- **Orchestration:** Apache Airflow
- **Storage:**
  - PostgreSQL (Staging & Warehouse databases)
  - Minio (Data lake for extracted and validated data)
- **Containerization:** Docker Image, Docker Compose
- **Visualization & Dashboard:** Metabase
- **Alerting:** Slack

---

## How to Run the Pipeline

1. **Prerequisites:**
   - Docker & Docker Compose installed

2. **Clone the Repository:**
   ```bash
   git clone https://github.com/oscar-sinaga/Startup-Ecosystem-Pipeline-using-Airflow-Spark.git
   cd Startup-Ecosystem-Pipeline-using-Airflow-Spark
   ```
3. **Create `.env` File in Project Root:**

   *Example:*

    ```bash
   AIRFLOW_UID=50000
   AIRFLOW_FERNET_KEY='rBEckHti5AkaGaAxQOMiXsC1FuZoufNy0I5xdBRP684='
   AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow_metadata/airflow
   AIRFLOW_DB_USER=airflow
   AIRFLOW_DB_PASSWORD=airflow
   AIRFLOW_DB_NAME=airflow
   AIRFLOW_PORT=5433

   STARTUP_INVESTMENTS_DB_USER=postgres
   STARTUP_INVESTMENTS_DB_PASSWORD=postgres
   STARTUP_INVESTMENTS_DB_NAME=startup_investments_db
   STARTUP_INVESTMENTS_PORT=5434

   STAGING_DB_USER=postgres
   STAGING_DB_PASSWORD=postgres
   STAGING_DB_NAME=staging_db
   STAGING_PORT=5435

   WAREHOUSE_DB_USER=postgres
   WAREHOUSE_DB_PASSWORD=postgres
   WAREHOUSE_DB_NAME=warehouse_db
   WAREHOUSE_PORT=5437

   LOG_DB_USER=postgres
   LOG_DB_PASSWORD=postgres
   LOG_DB_NAME=log_db
   LOG_PORT=5436

   MINIO_ROOT_USER=minio
   MINIO_ROOT_PASSWORD=minio123

   MB_DB_TYPE=postgres
   MB_DB_DBNAME=metabase_db
   MB_DB_PORT=5432
   MB_DB_USER=metabase_user
   MB_DB_PASS=metabase_pass
   MB_DB_HOST=metabase-db

   MB_ADMIN_EMAIL=admin@example.com
   MB_ADMIN_PASSWORD=admin123456
    ```

4. **Create Spreadsheet Credentials**

    First, create credentials for accessing the spreadsheet. The files `data/source/people.csv` and `data/source/relationships.csv` need to be uploaded to Google Drive and converted into spreadsheets. API access to these spreadsheets will be obtained using credentials generated from **Google Cloud Platform**.


5. **Save the Credentials**

    Save the credentials file into the `dags/include` folder.


6. **Build and Start Services**

    Run the following command to build the images and start all services (Spark, PostgreSQL, Minio, Airflow) in the background:

    ```bash
    docker-compose up -d --build
    ```

7. **Add Connections and Variables**
   
    Add configuration settings such as `Connections` and `Variables` so that Airflow can connect to multiple sources like **PostgreSQL Database**, **Minio Data Lake**, or preprocessing tools like **Apache Spark**:
    
    ```bash
    docker exec -it airflow_standalone bash
    cd dags/include
    airflow connections import connections.yaml
    airflow variables import variables.json
    exit
    ```

8. **Trigger ETL Job**

    To run the pipeline:  
    - Open the **Airflow Webserver** at `localhost:8082` (adjust according to your Airflow webserver port).  
    - Log in to Airflow using the username and password from the **Airflow container logs**:  
      ![log_airflow_standalone](picture/log_airflow_standalone.png)  
    - Trigger one of the DAGs as shown below:  
      ![how_run_dag](picture/how_run_dag.png)  

---

9. **Create Dashboard**

    To create a dashboard or visualization:  
    - Open **Metabase** at `localhost:8003` (adjust according to your Metabase port).  
    - Enter the username/email and password (as configured in `.env`).  
    - Start creating visualizations and dashboards. Example:  
      ![contoh_dashboard_metabase](picture/contoh_dashboard_metabase.png)  
