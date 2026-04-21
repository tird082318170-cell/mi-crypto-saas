import os
import requests
import psycopg2
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from threading import Thread
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

def motor_de_datos():
    monedas = "bitcoin,ethereum,solana,binancecoin,cardano,ripple"
    while True:
        try:
            if DATABASE_URL:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                cur.execute('''CREATE TABLE IF NOT EXISTS crypto_history 
                    (id SERIAL PRIMARY KEY, coin VARCHAR(50), price_usd FLOAT, 
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                res = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={monedas}&vs_currencies=usd&include_24hr_change=true", timeout=10)
                data = res.json()
                
                for coin, info in data.items():
                    cur.execute("INSERT INTO crypto_history (coin, price_usd) VALUES (%s, %s)", (coin, info['usd']))
                
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(60)

@app.on_event("startup")
def startup():
    Thread(target=motor_de_datos, daemon=True).start()

@app.get("/api/v1/mercado/resumen")
def obtener_resumen():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        query = "SELECT DISTINCT ON (coin) coin, price_usd FROM crypto_history ORDER BY coin, timestamp DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
