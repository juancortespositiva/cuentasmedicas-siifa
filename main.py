"""
Modulo: facturacion_siifa

Descripcion:
Servicio de integración entre BigQuery y SIIFA que:
1. Lee datos desde una vista en BigQuery
2. Autentica contra SIIFA
3. Envía facturas vía API (POST masivo)
4. Registra resultados en BigQuery (TRUNCATE + INSERT)
5. Inserta auditoría de trazabilidad
6. Genera archivo Excel
7. Almacena evidencia en GCS

Autor: VP Tecnica - Positiva Compania de Seguros
Fecha: 2026-03-18
"""

from google.cloud import bigquery, storage
import pandas as pd
from datetime import datetime
from io import BytesIO
from flask import Flask
import logging
import uuid
import requests
import os
from dotenv import load_dotenv
import hashlib

# ==============================
# LOAD ENV
# ==============================
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")

VIEW = "vw_facturacion_siifa_dian"
TABLA_AUDITORIA = "auditoria_siifa_envios"
TABLA_RESULTADOS = "resultado_siifa"

BUCKET_NAME = os.getenv("BUCKET_NAME")
DESTINO_BLOB = os.getenv("DESTINO_BLOB")

BASE_AUTH = os.getenv("SIIFA_BASE_AUTH")
BASE_API = os.getenv("SIIFA_BASE_API")

USERNAME = os.getenv("SIIFA_USER")
PASSWORD = os.getenv("SIIFA_PASSWORD")

logging.basicConfig(level=logging.INFO)

# ==============================
# 1. LEER BIGQUERY
# ==============================
def leer_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{VIEW}`
    """

    df = client.query(query, location="US").to_dataframe()
    logging.info(f"Registros leidos: {len(df)}")

    return df

# ==============================
# 2. LOGIN SIIFA
# ==============================
def login():
    print("🔐 Login SIIFA...")

    r = requests.post(
        f"{BASE_AUTH}/api/Auth/login",
        json={
            "userName": USERNAME,
            "password": PASSWORD  # 👈 TEXTO PLANO
        }
    )

    if r.status_code != 200:
        print("❌ Error login:", r.text)
        return None

    print("✅ Login OK\n")
    return r.json()["token"]

# ==============================
# 3. PAYLOAD
# ==============================
def construir_payload(df):
    return {
        "listaRadicado": [
            {
                "numeroFactura": row["Numero_factura"],
                "nitEmisor": row["ID_emisor"],
                "nitAdquiriente": row["ID_adquiriente"],
                "fechaRadicado": datetime.now().isoformat(),
                "radicado": f"RAD-{row['Numero_factura']}"
            }
            for _, row in df.iterrows()
        ]
    }

# ==============================
# 4. ENVIAR
# ==============================
def enviar_siifa(df, token):
    url = f"{BASE_API}/api/FacturaRadicado/Masivo"

    response = requests.post(
        url,
        json=construir_payload(df),
        headers={"Authorization": f"Bearer {token}"},
        timeout=60
    )

    return response.status_code, response.text

# ==============================
# 5. TRUNCATE
# ==============================
def truncate_resultados():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET}.{TABLA_RESULTADOS}`"
    client.query(query, location="US").result()

# ==============================
# 6. GUARDAR RESULTADO
# ==============================
def guardar_resultado(response_text, status):
    client = bigquery.Client(project=PROJECT_ID)

    df_res = pd.DataFrame([{
        "id": str(uuid.uuid4()),
        "fecha": pd.to_datetime(datetime.now()),  # TIMESTAMP OK
        "status": int(status),  # INTEGER OK
        "response": str(response_text)
    }])

    tabla = f"{PROJECT_ID}.{DATASET}.{TABLA_RESULTADOS}"

    job = client.load_table_from_dataframe(df_res, tabla)
    job.result()

# ==============================
# 7. AUDITORIA (FIX COMPLETO)
# ==============================
def insertar_auditoria(df, response_text):

    client = bigquery.Client(project=PROJECT_ID)

    df_aud = pd.DataFrame()

    df_aud["id_registro"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df_aud["identificador_factura"] = df["Identificador_de_factura"].astype(str)
    df_aud["numero_factura"] = df["Numero_factura"].astype(str)
    df_aud["id_cuenta_nur"] = df["IdCuenta_Nur"].astype(str)
    df_aud["id_emisor"] = df["ID_emisor"].astype(str)
    df_aud["id_adquiriente"] = df["ID_adquiriente"].astype(str)

    #  FIX TIPOS
    df_aud["valor_total"] = pd.to_numeric(df["Valor_total"], errors="coerce")
    df_aud["pagos_previos"] = pd.to_numeric(df["Pagos_previos"], errors="coerce").fillna(0).astype("int64")

    df_aud["fecha_emision"] = pd.to_datetime(df["Fecha_emision"], errors="coerce").dt.date
    df_aud["fecha_vencimiento"] = pd.to_datetime(df["Fecha_vencimiento"], errors="coerce").dt.date

    df_aud["estado"] = "ENVIADO"
    df_aud["tipo_operacion"] = "INSERT"
    df_aud["mensaje"] = "Envio SIIFA"
    df_aud["request_json"] = None
    df_aud["response_json"] = str(response_text)
    df_aud["fecha_proceso"] = pd.to_datetime(datetime.now())  # TIMESTAMP OK
    df_aud["usuario"] = "cloud_run"
    df_aud["origen"] = "API"

    tabla = f"{PROJECT_ID}.{DATASET}.{TABLA_AUDITORIA}"

    job = client.load_table_from_dataframe(df_aud, tabla)
    job.result()

    logging.info("Auditoria registrada OK")

# ==============================
# 8. EXCEL
# ==============================
def generar_excel(df):
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    nombre = f"siifa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    return buffer, nombre

def subir_gcs(buffer, nombre):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(f"{DESTINO_BLOB}/{nombre}")
    blob.upload_from_file(buffer)

# ==============================
# MAIN
# ==============================
def main():
    df = leer_bigquery()

    if df.empty:
        return "Sin datos"

    df = df.head(5)  # test

    token = login()

    status, response = enviar_siifa(df, token)

    truncate_resultados()
    guardar_resultado(response, status)
    insertar_auditoria(df, response)

    buffer, nombre = generar_excel(df)
    subir_gcs(buffer, nombre)

    return f"OK {status}"

# ==============================
# API
# ==============================
app = Flask(__name__)

@app.route("/v1/siifa")
def ejecutar():
    try:
        return main()
    except Exception as e:
        logging.error(str(e))
        return str(e), 500

# ==============================
# ENTRYPOINT
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
