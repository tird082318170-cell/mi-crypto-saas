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
    """Descarga múltiples monedas y calcula señales básicas"""
    monedas = "bitcoin,ethereum,solana,cardano"
    while True:
        try:
            if DATABASE_URL:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                cur.execute('''CREATE TABLE IF NOT EXISTS crypto_history 
                    (id SERIAL PRIMARY KEY, coin VARCHAR(50), price_usd FLOAT, 
                    media_movil FLOAT, var_porcentual FLOAT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                res = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={monedas}&vs_currencies=usd&include_24hr_change=true", timeout=10)
                data = res.json()
                
                for coin in data:
                    precio = data[coin]['usd']
                    cambio_24h = data[coin]['usd_24h_change']
                    
                    # Obtenemos media simple de los últimos registros
                    cur.execute("SELECT price_usd FROM crypto_history WHERE coin=%s ORDER BY timestamp DESC LIMIT 10", (coin,))
                    rows = cur.fetchall()
                    precios_anteriores = [r[0] for r in rows] if rows else [precio]
                    media = sum(precios_anteriores) / len(precios_anteriores)

                    cur.execute("INSERT INTO crypto_history (coin, price_usd, media_movil, var_porcentual) VALUES (%s, %s, %s, %s)",
                               (coin, precio, media, cambio_24h))
                
                conn.commit()
                cur.close()
                conn.close()
                print(f"✔️ Multimercado actualizado: {list(data.keys())}")
        except Exception as e:
            print(f"❌ Error: {e}")
        time.sleep(60)

@app.on_event("startup")
def startup():
    Thread(target=motor_de_datos, daemon=True).start()

@app.post("/api/v1/login")
def login(datos: LoginRequest):
    if datos.usuario == "admin" and datos.password == "crypto2026":
        return {"access_token": "token-pro-456", "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Error")

@app.get("/api/v1/mercado")
def obtener_mercado():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        # Obtenemos el último precio de cada moneda
        query = """
            SELECT DISTINCT ON (coin) coin, price_usd, media_movil, var_porcentual 
            FROM crypto_history 
            ORDER BY coin, timestamp DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
