# 🚚 Real-Time Logistics Delay Predictor

## 📌 Overview

A real-time logistics monitoring and delay prediction system that leverages streaming pipelines and machine learning to simulate delivery conditions and predict potential delays for improved operational efficiency.

---

## ⚙️ Tech Stack

* **Apache Kafka** – Real-time data streaming
* **Apache Spark (PySpark)** – Stream processing
* **Python** – Data simulation & ML model
* **Streamlit** – Dashboard visualization
* **Docker** – Containerized setup

---

## 📂 Project Structure

```
supply_chain_monitor/
│── api.py                 # API for serving predictions
│── dashboard.py           # Streamlit dashboard
│── data_generator.py      # Simulates telemetry data (speed, weather, traffic)
│── producer.py            # Kafka producer (sends data)
│── spark_processor.py     # Spark streaming + processing
│── train_model.py         # ML model training script
│── rf_model.pkl           # Trained Random Forest model
│── model_columns.pkl      # Model feature columns
│── docker-compose.yml     # Kafka + Zookeeper setup
│── validationCheck.ipynb  # Model validation notebook

```

---

## 🔍 Features

* Real-time streaming pipeline using Kafka and Spark
* Simulation of telemetry metrics (speed, weather, traffic)
* ML-based delay prediction using trained Random Forest model
* Interactive dashboard with filtering and monitoring
* API integration for real-time predictions
* Scalable architecture with scope for real-world data integration

---

## 🏗️ Architecture Workflow

1. **data_generator.py** simulates logistics telemetry
2. **producer.py** streams data to Kafka
3. **spark_processor.py** processes streaming data
4. ML model predicts delays
5. **api.py** serves predictions
6. **dashboard.py** visualizes results

---

## 🚀 Getting Started

### Prerequisites

* Python 3.x
* Apache Kafka & Zookeeper (or Docker)
* Apache Spark

---

### 🔧 Setup Instructions

```bash
# Clone repository
git clone https://github.com/novnishtha/Logistics-Delay-Predictor.git

cd Logistics-Delay-Predictor

# Install dependencies
pip install -r requirements.txt
```

---

### ▶️ Run the Project

#### 1. Start Kafka (Docker)

```bash
docker-compose up -d
```

#### 2. Start Data Generator & Producer

```bash
python data_generator.py
python producer.py
```

#### 3. Run Spark Streaming

```bash
python spark_processor.py
```

#### 4. Start API

```bash
python api.py
```

#### 5. Launch Dashboard

```bash
streamlit run dashboard.py
```

---

## 📊 Future Scope

* Alert system for proactive delay management
* Integration with real-time logistics and weather APIs
* Deployment on cloud platforms (AWS/GCP)
* Advanced ML/DL models for better accuracy


---

## 📄 License

This project is for educational and academic use.
