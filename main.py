"""
Modulo: facturacion_siifa

Descripcion:
Este modulo implementa un proceso ETL ligero que:
1. Consulta datos desde BigQuery (vista base)
2. Inserta registros en una tabla de auditoria evitando duplicados
3. Genera un archivo Excel en memoria
4. Carga el archivo a Cloud Storage
5. Expone el proceso mediante un endpoint Flask (Cloud Run)

Autor: VP Tecnica - Positiva Compania de Seguros
Fecha: 2026-03-24
"""

from google.cloud import bigquery, storage
import pandas as pd
from datetime import datetime
from io import BytesIO
from flask import Flask
import logging
import uuid

# ==============================
# CONFIGURACION
# ==============================
PROJECT_ID = "analitica-contact-center-dev"
DATASET = "CUENTAS_MEDICAS"
VIEW = "vw_facturacion_siifa_dian"
TABLA_AUDITORIA = "auditoria_siifa_envios"

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
    Consulta datos desde la vista base en BigQuery.

    Returns:
        pandas.DataFrame: Datos obtenidos de la vista.
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{VIEW}`
    """

    df = client.query(query).to_dataframe()
    logging.info(f"Registros leidos desde BigQuery: {len(df)}")

    return df


# ==============================
# 2. INSERTAR AUDITORIA SIN DUPLICADOS
# ==============================
def insertar_auditoria(df):
    """
    Inserta registros en la tabla de auditoria evitando duplicados
    basados en Numero_factura.

    Args:
        df (pandas.DataFrame): Datos de entrada.
    """
    client = bigquery.Client(project=PROJECT_ID)

    if df.empty:
        logging.info("No hay datos para auditoria")
        return

    tabla_destino = f"{PROJECT_ID}.{DATASET}.{TABLA_AUDITORIA}"

    # ==============================
    # 1. CONSULTAR FACTURAS EXISTENTES
    # ==============================
    query_existentes = f"""
        SELECT Numero_factura
        FROM `{tabla_destino}`
    """

    df_existentes = client.query(query_existentes).to_dataframe()

    if not df_existentes.empty:
        facturas_existentes = set(df_existentes["Numero_factura"].astype(str))
        df = df[~df["Numero_factura"].astype(str).isin(facturas_existentes)]

    logging.info(f"Registros nuevos a insertar: {len(df)}")

    if df.empty:
        logging.info("No hay registros nuevos (todos son duplicados)")
        return

    # ==============================
    # 2. PREPARAR DATAFRAME DE AUDITORIA
    # ==============================
    df_aud = df.copy()

    df_aud["id_registro"] = [str(uuid.uuid4()) for _ in range(len(df_aud))]
    df_aud["estado"] = "SIMULADO"
    df_aud["tipo_operacion"] = "INSERT"
    df_aud["mensaje"] = "Simulacion exitosa"
    df_aud["request_json"] = None
    df_aud["response_json"] = None
    df_aud["fecha_proceso"] = datetime.now()
    df_aud["usuario"] = "cloud_run"
    df_aud["origen"] = "API"

    # ==============================
    # 3. INSERTAR EN BIGQUERY
    # ==============================
    job = client.load_table_from_dataframe(df_aud, tabla_destino)
    job.result()

    logging.info(f"Registros insertados en auditoria: {len(df_aud)}")


# ==============================
# 3. GENERAR EXCEL EN MEMORIA
# ==============================
def generar_excel_en_memoria(df):
    """
    Genera un archivo Excel en memoria.

    Args:
        df (pandas.DataFrame)

    Returns:
        tuple: buffer y nombre del archivo
    """
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SIIFA')

    buffer.seek(0)

    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"facturacion_siifa_{fecha}.xlsx"

    logging.info(f"Excel generado: {nombre_archivo}")

    return buffer, nombre_archivo


# ==============================
# 4. SUBIR A GCS
# ==============================
def subir_gcs_desde_memoria(buffer, nombre_archivo):
    """
    Sube archivo a Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(f"{DESTINO_BLOB}/{nombre_archivo}")

    blob.upload_from_file(
        buffer,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    logging.info(f"Archivo subido a GCS: {nombre_archivo}")


# ==============================
# 5. PROCESO PRINCIPAL
# ==============================
def main():
    """
    Ejecuta el flujo completo:
    - Lee datos
    - Inserta auditoria sin duplicados
    - Genera Excel
    - Sube a GCS
    """
    df = leer_bigquery()

    if df.empty:
        logging.info("No hay datos para procesar")
        return "Sin datos"

    insertar_auditoria(df)

    buffer, nombre_archivo = generar_excel_en_memoria(df)
    subir_gcs_desde_memoria(buffer, nombre_archivo)

    return f"Proceso completado - {len(df)} registros evaluados"


# ==============================
# 6. API FLASK
# ==============================
app = Flask(__name__)

@app.route("/v1/generar-siifa")
def ejecutar():
    """
    Endpoint HTTP para ejecutar el proceso.
    """
    try:
        resultado = main()
        return resultado
    except Exception as e:
        logging.error(f"Error en ejecucion: {str(e)}")
        return f"Error: {str(e)}", 500


# ==============================
# ENTRYPOINT
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
