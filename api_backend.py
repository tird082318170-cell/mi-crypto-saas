import os
import requests
import psycopg2
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

def motor_de_datos():
    # Lista de monedas a rastrear
    monedas = "bitcoin,ethereum,solana,binancecoin"
    while True:
        try:
            if DATABASE_URL:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                
                # Aseguramos que la tabla soporte los datos
                cur.execute('''CREATE TABLE IF NOT EXISTS crypto_history 
                    (id SERIAL PRIMARY KEY, coin VARCHAR(50), price_usd FLOAT, 
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
                
                res = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={monedas}&vs_currencies=usd", timeout=10)
                data = res.json()
                
                for coin, info in data.items():
                    cur.execute("INSERT INTO crypto_history (coin, price_usd) VALUES (%s, %s)", (coin, info['usd']))
                
                conn.commit()
                cur.close()
                conn.close()
        except Exception as e:
            print(f"Error en ingesta: {e}")
        time.sleep(60) # Actualiza cada minuto

@app.on_event("startup")
def startup():
    Thread(target=motor_de_datos, daemon=True).start()

@app.get("/api/v1/mercado/completo")
def obtener_mercado():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        # Obtenemos los últimos 20 registros de cada moneda para la gráfica
        query = """
            SELECT coin, price_usd, timestamp 
            FROM (
                SELECT coin, price_usd, timestamp,
                ROW_NUMBER() OVER (PARTITION BY coin ORDER BY timestamp DESC) as rn
                FROM crypto_history
            ) t WHERE rn <= 20
            ORDER BY coin, timestamp ASC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Agrupamos por moneda para facilitar el trabajo al frontend
        resultado = {}
        for coin in df['coin'].unique():
            sub_df = df[df['coin'] == coin]
            resultado[coin] = {
                "actual": sub_df['price_usd'].iloc[-1],
                "historial": sub_df['price_usd'].tolist()
            }
        return resultado
    except Exception as e:
        return {"error": str(e)}
