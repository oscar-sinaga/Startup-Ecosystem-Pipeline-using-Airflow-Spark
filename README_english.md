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