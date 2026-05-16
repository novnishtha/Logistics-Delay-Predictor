# 🚚 LogiStream – Real-Time Logistics Delay Predictor

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-black.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.25+-FF4B4B.svg)

## 📖 1. Project Overview
**LogiStream** is a real-time logistics tracking and delay prediction system. It simulates a fleet of trucks generating live telematics data (speed, distance, weather, traffic), streams the data through **Apache Kafka**, processes it with **Apache Spark Structured Streaming**, and applies a Machine Learning model (**Random Forest**) to predict delays in real-time. The processed data is stored in **PostgreSQL** and served via **FastAPI** to a live **Streamlit Dashboard**.

## ✨ 2. Features
- **Real-Time Data Streaming:** Simulates live truck telematics and streams them via Apache Kafka.
- **Machine Learning Integration:** Uses a pre-trained Random Forest model to predict delays based on weather, traffic, speed, and distance.
- **Stream Processing:** PySpark Structured Streaming processes data, applies Pandas UDFs for distributed inference, and computes ETAs.
- **Database Upserts:** Real-time deduplication and UPSERT operations into PostgreSQL.
- **RESTful API:** FastAPI provides endpoints to query truck status and delays.
- **Live Dashboard:** Streamlit dashboard for real-time fleet monitoring and identifying delayed trucks.

## 🛠️ 3. Tech Stack
- **Data Generation & ML:** Python, Scikit-learn, Pandas, Joblib
- **Message Broker:** Apache Kafka, Zookeeper (Dockerized)
- **Stream Processing:** Apache Spark (PySpark)
- **Database:** PostgreSQL (Dockerized)
- **Backend API:** FastAPI, Psycopg2
- **Frontend Dashboard:** Streamlit
- **Containerization:** Docker Compose

## 🏗️ 4. System Architecture
1. **Producer (`producer.py`)**: Simulates 30 trucks and sends JSON payloads to the Kafka topic `logistics_events`.
2. **Spark Processor (`spark_processor.py`)**: Consumes Kafka stream, extracts JSON, applies the ML model via `pandas_udf` to predict delays, calculates ETAs, and writes the results to PostgreSQL staging tables. It then performs SQL UPSERTS to update the final tables.
3. **Database (PostgreSQL)**: Stores the latest status and ETA for each truck (`truck_eta` and `truck_status` tables).
4. **API (`api.py`)**: FastAPI connects to PostgreSQL using a connection pool and exposes REST endpoints.
5. **Dashboard (`dashboard.py`)**: Streamlit app fetches data from FastAPI and visualizes it.

## 🔄 5. Project Workflow
`Data Generator` -> `Kafka Producer` -> `Spark Streaming (ML Inference + ETA Calculation)` -> `PostgreSQL` -> `FastAPI` -> `Streamlit Dashboard`

## 📂 6. Folder Structure
```text
logistics_delay_predictor/
│── checkpoints/            # Spark streaming checkpoints
│── docker-compose.yml      # Kafka, Zookeeper, and PostgreSQL setup
│── data_generator.py       # Logic for generating simulated logistics data
│── train_model.py          # Trains and saves the Random Forest model
│── producer.py             # Kafka producer simulating live truck data
│── spark_processor.py      # PySpark streaming job for data processing
│── api.py                  # FastAPI application serving database records
│── dashboard.py            # Streamlit dashboard for data visualization
│── rf_model.pkl            # Trained Random Forest model
│── model_columns.pkl       # Saved feature columns for model inference
└── README.md               # Project documentation
```

## ⚙️ 7. Installation Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/novnishtha/Logistics-Delay-Predictor/
   cd logistics_delay_predictor
   ```
2. **Install dependencies:**
   Ensure you have Python 3.8+ installed. Install required packages:
   ```bash
   pip install pyspark pandas scikit-learn kafka-python psycopg2-binary fastapi uvicorn streamlit joblib
   ```
3. **Ensure Docker is installed and running.**

## 🐳 8. Kafka & Database Setup
Start Zookeeper, Kafka, and PostgreSQL containers using Docker Compose:
```bash
docker-compose up -d
```
*Note: This creates a database named `logistics_db` with user/password `admin`/`admin`.*

Before running the Spark processor, ensure the PostgreSQL tables exist. Connect to PostgreSQL and create the necessary tables:
```sql
CREATE TABLE truck_eta (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    last_update TIMESTAMP,
    current_speed FLOAT,
    remaining_distance_km FLOAT,
    predicted_delay FLOAT,
    eta_minutes FLOAT
);

CREATE TABLE truck_status (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    last_update TIMESTAMP,
    status VARCHAR(50)
);

CREATE TABLE truck_eta_staging (LIKE truck_eta);
CREATE TABLE truck_status_staging (LIKE truck_status);
```

## ⚡ 9. Spark Streaming Setup
Make sure you have Spark configured and the required JAR packages for Kafka and PostgreSQL integration. The Spark processor automatically downloads necessary packages on startup.

## 🚀 10. Running the Project
Open separate terminal windows for each of the following components:

**Terminal 1: Train the ML Model (If not already trained)**
```bash
python train_model.py
```

**Terminal 2: Start the Kafka Producer**
```bash
python producer.py
```

**Terminal 3: Start the Spark Streaming Processor**
```bash
python spark_processor.py
```

**Terminal 4: Start the FastAPI Server**
```bash
uvicorn api:app --reload --port 8000
```

**Terminal 5: Start the Streamlit Dashboard**
```bash
streamlit run dashboard.py
```

## 🔌 11. API Endpoints
The FastAPI server runs at `http://127.0.0.1:8000`. You can view the Swagger UI at `http://127.0.0.1:8000/docs`.
- `GET /` - Check if API is running.
- `GET /vehicles` - Get a list of all vehicles with their ETA and status.
- `GET /vehicles/{vehicle_id}` - Get details for a specific vehicle.
- `GET /vehicles/status/{status}` - Filter vehicles by status (`IN_PROGRESS`, `COMPLETED`).
- `GET /delays` - Get a list of delayed vehicles (ETA > 60 mins).

## 📊 12. Dashboard Features
The Streamlit dashboard (`http://localhost:8501`) includes:
- **Live Metrics:** Total Trucks, In Progress, Completed.
- **Filterable Data Grid:** View all truck states and filter by status.
- **Delay Highlighting:** Rows with extreme delays (ETA > 150 mins) are highlighted in red.
- **Top Delayed Trucks:** A specialized view showing the top 5 most delayed vehicles.
- **Auto-Refresh:** Configurable sidebar slider to automatically refresh data every few seconds.

## 📈 13. Sample Data Flow
1. **Producer sends:** `{"timestamp": 1716132000.0, "vehicle_id": "TRUCK_101", "distance_km": 150.5, "speed_kmh": 65.2, "weather": "Rainy", "traffic": "Heavy", "delay_minutes": 12.5}`
2. **Spark processes:** Enriches with `predicted_delay` using Random Forest and calculates `eta_minutes`.
3. **API returns:** JSON payload including formatted timestamps and calculated `status`.
4. **Dashboard visualizes:** Updating table with color-coded alerts for delayed trucks.

## 🚀 14. Future Improvements
- Integrate historical batch processing to retrain the ML model dynamically.
- Add authentication to the FastAPI endpoints.
- Enhance the Streamlit dashboard with geographic map visualizations for truck tracking.
- Deploy the entire stack using Kubernetes for better scalability.
