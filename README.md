# ecommerce-data-pipeline

# 🛒 Real-Time E-Commerce Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0.0-blue)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-black)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.8.0-green)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

An end-to-end production-grade streaming data pipeline that ingests real-time
e-commerce orders, processes them through a Medallion Architecture
(Bronze → Silver → Gold), orchestrates with Apache Airflow, and syncs to AWS S3.

---

## 🏗️ Architecture

    Kafka Producer (5 orders/sec)
         ↓
    Apache Kafka (orders_topic)
         ↓
    Spark Structured Streaming
         ↓
    Bronze Layer  →  Raw Delta Tables
         ↓
    Silver Layer  →  Cleaned & Validated
         ↓
    Gold Layer    →  Business Aggregations
         ↓
    AWS S3 (Cloud Storage)
         ↓
    Apache Airflow (Daily Orchestration)

---

## 🛠️ Tech Stack

| Layer            | Technology                     |
| ---------------- | ------------------------------ |
| Ingestion        | Apache Kafka 3.4, kafka-python |
| Processing       | Apache Spark 3.5, PySpark      |
| Storage          | Delta Lake 3.0, AWS S3         |
| Orchestration    | Apache Airflow 2.8             |
| Containerization | Docker, Docker Compose         |
| Data Quality     | Great Expectations             |
| Cloud            | AWS S3, Google Colab           |
| Language         | Python 3.11                    |

---

### 📂 Project Structure

```text
ecommerce-data-pipeline/
├── 📂 data/                  # Medallion Storage Layer (Mounted Volumes)
│   ├── 🥉 bronze/            # Ingestion: Raw Parquet/Delta files
│   ├── 🥈 silver/            # Processing: Cleaned & Unified Delta tables
│   ├── 🥇 gold/              # Aggregation: Business-ready KPIs
│   └── 🏁 checkpoints/       # Spark Streaming offsets (Fault Tolerance)
├── 📂 docs/                  # Architectural diagrams & design specs
├── 📂 src/                   # Core Pipeline Logic
│   ├── 🌬️ airflow/           # DAG definitions & scheduling
│   ├── 🥉 bronze/            # Raw ingestion & Kafka consumption
│   ├── 🥈 silver/            # Cleaning, deduplication & Delta logic
│   ├── 🥇 gold/              # Complex Spark SQL aggregations
│   ├── 🛰️ producer/          # Kafka event simulation
│   ├── 🧪 quality/           # Data validation (Great Expectations)
│   └── ☁️ cloud/              # AWS S3 integration & Boto3 scripts
├── 📂 tests/                 # Unit & Integration test suites
├── 🐳 Dockerfile             # Custom Spark + Airflow image
├── 🐋 docker-compose.yml     # Infrastructure (Kafka, Airflow, Postgres)
├── 📜 log4j.properties       # Spark logging configuration
├── 🐍 requirements.txt       # Python dependencies
└── 📖 README.md              # Project documentation
```
---

## 🚀 Pipeline Stages

### 1. Data Ingestion (Kafka Producer)

- Generates realistic Indian e-commerce orders using Faker
- Produces 5 orders/second to `orders_topic` Kafka topic
- Order fields: order_id, user_id, product, city, amount, payment method
- 10 Indian cities, 10 products, 5 payment methods

### 2. Bronze Layer (Spark Structured Streaming)

- Reads from Kafka every 10 seconds (micro-batch)
- Writes raw JSON as Delta Lake tables
- Tracks Kafka offset, partition, ingestion timestamp
- Fault-tolerant via checkpointing
- **2,245+ orders ingested**

### 3. Silver Layer (Spark Batch)

- Removes duplicate orders using `order_id`
- Drops records with null critical fields
- Adds derived columns: `order_date`, `amount_category`, `is_cancelled`, `is_delivered`
- Partitioned by `order_date` for fast queries
- **2,243 clean records**

### 4. Gold Layer (Spark Batch)

- 6 business aggregation tables:
  - `daily_revenue_by_city` — revenue trends by location
  - `top_products` — best selling products by revenue
  - `order_status_summary` — order funnel analysis
  - `payment_method_analysis` — UPI vs Card vs COD split
  - `city_performance_ranking` — city-wise ranked performance
  - `amount_category_summary` — Low/Medium/High/Premium split

### 5. Airflow Orchestration

- Daily DAG runs at 6 AM automatically
- 8-task pipeline with dependency management:
  `start → bronze_health → silver → gold → s3_upload → quality → notification → end`
- Retry logic: 2 retries with 5-minute delay
- Email alerts on failure

### 6. AWS S3 Sync

- All Delta tables automatically synced to AWS S3
- Bucket: Mumbai region (ap-south-1)
- Structure: `s3://bucket/bronze/`, `/silver/`, `/gold/`

### 7. Data Quality

- 20+ automated checks across all 3 layers
- Validates: nulls, duplicates, amounts, status values, payment methods
- Warning system for high cancellation rates
- All checks pass ✅

---

## 📊 Key Metrics

| Metric             | Value        |
| ------------------ | ------------ |
| Orders ingested    | 2,245+       |
| Clean records      | 2,243        |
| Duplicates removed | 2            |
| Gold tables        | 6            |
| Quality checks     | 20+          |
| Pipeline tasks     | 8            |
| Ingestion rate     | 5 orders/sec |

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.11+
- Java 11+
- Docker Desktop
- AWS Account (free tier)

### 1. Clone the repository

```bash
git clone https://github.com/Deepak-9105/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### 2. Create virtual environment

```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

### 5. Start infrastructure

```bash
docker compose up -d
```

### 6. Run the pipeline

**Start Kafka Producer (Terminal 1):**

```bash
python src/producer/order_producer.py
```

**Start Bronze Streaming (Terminal 2):**

```bash
python src/bronze/bronze_stream.py
```

**Run Silver transformation:**

```bash
python src/silver/silver_transform.py
```

**Run Gold aggregations:**

```bash
python src/gold/gold_aggregations.py
```

**Run Data Quality checks:**

```bash
python src/quality/data_quality_checks.py
```

**Or trigger Airflow DAG** at `http://localhost:8080`

---

## 🔑 Environment Variables

Create a `.env` file with:

```text
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-1
S3_BUCKET=your-bucket-name
```

---

## 📈 Airflow DAG

The pipeline runs daily at 6 AM with 8 tasks:
```text
pipeline_start
↓
check_bronze_health
↓
run_silver_transformation
↓
run_gold_aggregations
↓
upload_to_s3
↓
run_data_quality_checks
↓
pipeline_success_notification
↓
pipeline_end
```

---

## 🧠 What I Learned

- Building real-time streaming pipelines with Kafka + Spark Structured Streaming
- Implementing Medallion Architecture (Bronze → Silver → Gold) with Delta Lake
- Orchestrating complex workflows with Apache Airflow DAGs
- Managing cloud storage with AWS S3 and boto3
- Data quality validation across pipeline layers
- Containerizing data infrastructure with Docker Compose
- Handling Windows-specific PySpark challenges (HADOOP_HOME, winutils)

---

## 🔮 Future Improvements

- Add Terraform for AWS infrastructure as code
- Implement CDC (Change Data Capture) with Debezium
- Add Great Expectations HTML data docs
- Deploy Airflow on AWS MWAA
- Add Prometheus + Grafana for pipeline monitoring
- Implement schema evolution with Delta Lake
- Add dbt for SQL transformations on Gold layer

---

## 👨‍💻 Author

**Deepak**

- GitHub: [@Deepak-9105](https://github.com/Deepak-9105)
- Project: [ecommerce-data-pipeline](https://github.com/Deepak-9105/ecommerce-data-pipeline)
