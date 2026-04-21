import os
import requests
import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from threading import Thread
import time
from datetime import datetime

app = FastAPI()

# Permitir que el HTML se conecte a la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

class LoginRequest(BaseModel):
    usuario: str
    password: str

def motor_de_datos():
    """Hilo que crea la tabla y descarga datos cada 60 segundos"""
    while True:
        try:
            if DATABASE_URL:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                # Crear tabla con estructura completa
                cur.execute('''CREATE TABLE IF NOT EXISTS crypto_history 
                    (id SERIAL PRIMARY KEY, coin VARCHAR(50), price_usd FLOAT, 
                    media_movil FLOAT, var_porcentual FLOAT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                # Descargar precio real de Bitcoin
                res = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd", timeout=10)
                precio = res.json()['bitcoin']['usd']
                
                # Insertar datos (Media móvil simplificada al precio actual para el ejemplo)
                cur.execute("INSERT INTO crypto_history (coin, price_usd, media_movil, var_porcentual) VALUES (%s, %s, %s, %s)",
                           ('bitcoin', precio, precio, 0.0))
                
                conn.commit()
                cur.close()
                conn.close()
                print(f"✔️ Datos actualizados: ${precio}")
        except Exception as e:
            print(f"❌ Error en motor de datos: {e}")
        time.sleep(60)

@app.on_event("startup")
def startup():
    Thread(target=motor_de_datos, daemon=True).start()

@app.post("/api/v1/login")
def login(datos: LoginRequest):
    if datos.usuario == "admin" and datos.password == "crypto2026":
        return {"access_token": "token-secreto-123", "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")

@app.get("/api/v1/precios/bitcoin")
def obtener_precios():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        df = pd.read_sql("SELECT price_usd, media_movil, var_porcentual FROM crypto_history ORDER BY timestamp DESC LIMIT 1", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else {"price_usd": 0, "media_movil": 0, "var_porcentual": 0}
    except Exception as e:
        return {"error": str(e)}