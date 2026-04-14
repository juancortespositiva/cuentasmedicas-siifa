"""
Modulo: facturacion_siifa

Descripcion:
Este modulo implementa un proceso ETL ligero que:
1. Consulta datos desde BigQuery
2. Genera un archivo Excel en memoria
3. Carga el archivo a Cloud Storage
4. Expone el proceso mediante un endpoint Flask (Cloud Run)

Autor: VP Tecnica - Positiva Compania de Seguros
Fecha: 2026-03-24
"""

from google.cloud import bigquery, storage
import pandas as pd
from datetime import datetime
from io import BytesIO
from flask import Flask
import logging

# ==============================
# CONFIGURACION
# ==============================
PROJECT_ID = "analitica-contact-center-dev"
DATASET = "CUENTAS_MEDICAS"
VIEW = "vw_facturacion_siifa_dian"

BUCKET_NAME = "buckets-aws"
DESTINO_BLOB = "cuentasmedicas/facturacion_siifa"

# ==============================
# LOGS
# ==============================
logging.basicConfig(level=logging.INFO)


# ==============================
# 1. LEER BIGQUERY
# ==============================
def leer_bigquery():
    """
    Consulta datos desde una vista en BigQuery.

    Returns:
        pandas.DataFrame: Datos obtenidos de la vista configurada.
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{VIEW}`
    """

    df = client.query(query).to_dataframe()
    logging.info(f"Registros leidos: {len(df)}")

    return df


# ==============================
# 2. GENERAR EXCEL EN MEMORIA
# ==============================
def generar_excel_en_memoria(df):
    """
    Genera un archivo Excel en memoria a partir de un DataFrame.

    Args:
        df (pandas.DataFrame): Datos a exportar.

    Returns:
        tuple: (buffer en memoria, nombre del archivo generado)
    """
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SIIFA')

    buffer.seek(0)

    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"facturacion_siifa_{fecha}.xlsx"

    logging.info(f"Excel generado en memoria: {nombre_archivo}")

    return buffer, nombre_archivo


# ==============================
# 3. SUBIR A GCS DESDE MEMORIA
# ==============================
def subir_gcs_desde_memoria(buffer, nombre_archivo):
    """
    Carga un archivo desde memoria a Google Cloud Storage.

    Args:
        buffer (BytesIO): Archivo en memoria.
        nombre_archivo (str): Nombre del archivo destino.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(f"{DESTINO_BLOB}/{nombre_archivo}")

    blob.upload_from_file(
        buffer,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    logging.info(f"Archivo subido a gcs: gs://{BUCKET_NAME}/{DESTINO_BLOB}/{nombre_archivo}")


# ==============================
# 4. PROCESO PRINCIPAL
# ==============================
def main():
    """
    Orquesta el proceso completo:
    - Lectura de datos
    - Generacion de archivo
    - Carga a almacenamiento

    Returns:
        str: Resultado del proceso
    """
    df = leer_bigquery()

    if df.empty:
        logging.info("No hay datos para procesar N/A")
        return "Sin datos"

    buffer, nombre_archivo = generar_excel_en_memoria(df)
    subir_gcs_desde_memoria(buffer, nombre_archivo)

    return "Proceso completado"


# ==============================
# 5. FLASK (CLOUD RUN)
# ==============================
app = Flask(__name__)

@app.route("/v1/generar-siifa")
def ejecutar():
    """
    Endpoint principal para ejecutar el proceso.

    Returns:
        str: Resultado de la ejecucion
    """
    resultado = main()
    return resultado


# ==============================
# ENTRYPOINT CLOUD RUN
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
