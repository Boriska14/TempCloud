from fastapi import FastAPI
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, DateTime
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import paho.mqtt.client as mqtt
import json

DATABASE_URL = "postgresql://postgres:123456789@localhost/TempCloud"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

temperature_readings = Table(
    "temperature_readings",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("sensor_id", Integer, nullable=False),
    Column("temperature", Float, nullable=False),
    Column("timestamp", DateTime, default=datetime.utcnow)  # ajouté par le backend si pas fourni
)

metadata.create_all(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()

# --- MQTT ---
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    print("Message reçu :", payload)

    session = SessionLocal()
    try:
        insert_stmt = temperature_readings.insert().values(
            sensor_id=payload.get("sensor_id", 0),
            temperature=payload.get("temperature"),
            timestamp=datetime.utcnow()  # backend ajoute l’heure courante
        )
        session.execute(insert_stmt)
        session.commit()
        print(" Donnée enregistrée dans PostgreSQL")
    except Exception as e:
        session.rollback()
        print(" Erreur DB :", e)
    finally:
        session.close()

client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883)
client.subscribe("iot/sensor/temperature")
client.loop_start()

@app.get("/")
def root():
    return {"message": "Backend IoT actif"}

@app.get("/data")
def get_data():
    """Retourne les dernières valeurs stockées"""
    session = SessionLocal()
    try:
        result = session.execute(temperature_readings.select().order_by(temperature_readings.c.id.desc()).limit(10))
        rows = [dict(r) for r in result]
        return {"data": rows}
    finally:
        session.close()
