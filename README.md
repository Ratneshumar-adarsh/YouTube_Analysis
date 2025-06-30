# 📊 YouTube Data Analysis Pipeline
This project is a robust, end-to-end data engineering pipeline designed to extract, transform, and analyze YouTube channel and video data. Built entirely on Databricks, it uses Python, PySpark, SQL, and the Databricks Jobs API to deliver an automated, idempotent, and scalable solution.

## 🚀 Features
   🔁 Incremental & Idempotent Data Ingestion
   
   📚 Multi-Notebook Orchestration via Databricks Jobs API
   
   🔎 Data Quality Checks & Deduplication
   
   📈 Interactive Dashboards for Channel-Level Metrics
  
   ⏱ Scheduled Jobs via API (No Manual Triggers)
   

## 🧱 Pipeline Architecture
### 1. Notebook 1: Table Creation
      Creates necessary base, silver, and final tables.
      Ensures schema is consistent across runs.

### 2. Notebook 2: Data Ingestion & Transformation
      Ingests YouTube channel and video data.
      Includes:
            Data validation
            Type casting and normalization
            Deduplication logic
            Incremental load handling using publishedAt and primary keys.

### 3. Notebook 3: Dashboard Creation
  #### Aggregates data for visualization.
    Computes metrics such as:
        Most viewed, liked, and commented videos
        Video uploads per day/month
        Subscriber and video count trends

  #### 📊 Dashboard Highlights
        Top Performing Videos by Views, Likes, Comments
        Upload Frequency: Monthly & Daily granularity
        Channel Growth: Subscribers & Video Count
        Channel-wise Comparisons & Trends

## 🔧 Technologies Used
      Tool	                 Purpose
      Databricks             Development environment, orchestration
      PySpark                Distributed data processing
      SQL                    Data modeling & querying
      Python                 API interaction, orchestration logic
      Databricks Jobs API    Job scheduling and automation

## 🛠 Setup & Execution
      1. Import notebooks into your Databricks workspace.
      2. Create required Databricks cluster.
      3. Set up Databricks Job with three tasks:
          Table Creation
          Data Ingestion
          Dashboard Generation
      4. Use Databricks REST API to automate and schedule job runs.
      5. Monitor job output and view the dashboard for insights.

## 📅 Scheduling
      Jobs are scheduled using the Databricks Jobs API, and parameters such as run_date are passed dynamically to support incremental loads.

## 🧪 Data Quality
      Schema enforcement
      Null checks
      Duplicate detection and removal
      Validation of timestamp and key fields

## 📌 Future Enhancements
      Add unit tests using pytest or dbt tests
      Extend API integration to fetch additional metadata
      Add alerts for data anomalies or load failures

## 👨‍💻 Author
### Ratnesh Kumar | Data Engineer |
### [ratneshagkb11@gmail.com]
### [https://www.linkedin.com/in/ratnesh-kumar-a147b1195/]
### [https://github.com/Ratneshumar-adarsh]
