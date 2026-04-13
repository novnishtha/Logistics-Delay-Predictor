from fastapi import FastAPI
from psycopg2.pool import SimpleConnectionPool
from pydantic import BaseModel, field_serializer
from typing import List, Optional
from datetime import datetime

# 1. FastAPI App
app = FastAPI(title="Logistics Tracking API")

# 2. Connection Pool
pool = SimpleConnectionPool(
    1, 10,
    host="localhost",
    database="logistics_db",
    user="admin",
    password="admin"
)

def get_conn():
    return pool.getconn()

def release_conn(conn):
    pool.putconn(conn)

# 3. Response Model
class Vehicle(BaseModel):
    vehicle_id: str
    last_update: Optional[datetime]
    current_speed: Optional[float]
    remaining_distance_km: Optional[float]
    predicted_delay: Optional[float]
    eta_minutes: Optional[float]
    status: Optional[str]

    @field_serializer("last_update")
    def format_time(self, value):
        return value.strftime("%d %b %Y, %I:%M %p") if value else None

# 4. Root Endpoint
@app.get("/")
def home():
    return {"message": "Logistics API running"}

# 5. Get All Vehicles (JOIN)
@app.get("/vehicles", response_model=List[Vehicle])
def get_vehicles(limit: int = 30, offset: int = 0):

    conn = get_conn()
    cur = conn.cursor()

    query = """SELECT 
            e.vehicle_id,
            e.last_update,
            e.current_speed,
            e.remaining_distance_km,
            e.predicted_delay,
            e.eta_minutes,
            s.status
        FROM truck_eta e
        JOIN truck_status s
        ON e.vehicle_id = s.vehicle_id
        ORDER BY e.last_update DESC
        LIMIT %s OFFSET %s;
    """

    cur.execute(query, (limit, offset))
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    release_conn(conn)

    return result

# 6. Get Single Vehicle
@app.get("/vehicles/{vehicle_id}", response_model=Vehicle)
def get_vehicle(vehicle_id: str):

    conn = get_conn()
    cur = conn.cursor()

    query = """SELECT 
            e.vehicle_id,
            e.last_update,
            e.current_speed,
            e.remaining_distance_km,
            e.predicted_delay,
            e.eta_minutes,
            s.status
        FROM truck_eta e
        JOIN truck_status s
        ON e.vehicle_id = s.vehicle_id
        WHERE e.vehicle_id = %s;
    """

    cur.execute(query, (vehicle_id,))
    row = cur.fetchone()

    cur.close()
    release_conn(conn)

    if not row:
        return {"error": "Vehicle not found"}

    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, row))

# 7. Filter by Status
@app.get("/vehicles/status/{status}", response_model=List[Vehicle])
def get_by_status(status: str, limit: int = 30):

    conn = get_conn()
    cur = conn.cursor()

    query = """SELECT 
            e.vehicle_id,
            e.last_update,
            e.current_speed,
            e.remaining_distance_km,
            e.predicted_delay,
            e.eta_minutes,
            s.status
        FROM truck_eta e
        JOIN truck_status s
        ON e.vehicle_id = s.vehicle_id
        WHERE s.status = %s
        ORDER BY e.last_update DESC
        LIMIT %s;
    """

    cur.execute(query, (status, limit))
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    release_conn(conn)

    return result

# 8. Delayed Vehicles
@app.get("/delays", response_model=List[Vehicle])
def get_delayed(limit: int = 30):

    conn = get_conn()
    cur = conn.cursor()

    query = """SELECT 
            e.vehicle_id,
            e.last_update,
            e.current_speed,
            e.remaining_distance_km,
            e.predicted_delay,
            e.eta_minutes,
            s.status
        FROM truck_eta e
        JOIN truck_status s
        ON e.vehicle_id = s.vehicle_id
        WHERE e.eta_minutes > 60
        ORDER BY e.eta_minutes DESC
        LIMIT %s;"""

    cur.execute(query, (limit,))
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    result = [dict(zip(columns, row)) for row in rows]

    cur.close()
    release_conn(conn)

    return result