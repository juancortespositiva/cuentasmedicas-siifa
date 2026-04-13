from google.cloud import bigquery, storage
import pandas as pd
from datetime import datetime
from io import BytesIO

# CONFIGURACION
PROJECT_ID = "analitica-contact-center-dev"
DATASET = "CUENTAS_MEDICAS"
VIEW = "vw_facturacion_siifa_dian"

BUCKET_NAME = "buckets-aws"
DESTINO_BLOB = "cuentasmedicas/facturacion_siifa"

# 1. LEER BIGQUERY
def leer_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{VIEW}`
    """

    df = client.query(query).to_dataframe()
    print(f"Registros leidos: {len(df)}")

    return df

# 2. GENERAR EXCEL EN MEMORIA
def generar_excel_en_memoria(df):
    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="SIIFA")

    buffer.seek(0)

    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"facturacion_siifa_{fecha}.xlsx"

    print(f"Excel generado en memoria: {nombre_archivo}")

    return buffer, nombre_archivo

# 3. SUBIR A GCS DESDE MEMORIA
def subir_gcs_desde_memoria(buffer, nombre_archivo):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(f"{DESTINO_BLOB}/{nombre_archivo}")

    blob.upload_from_file(
        buffer,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    print(f"Archivo subido a: gs://{BUCKET_NAME}/{DESTINO_BLOB}/{nombre_archivo}")

# MAIN
def main():
    df = leer_bigquery()

    if df.empty:
        print("No hay datos para procesar")
        return

    buffer, nombre_archivo = generar_excel_en_memoria(df)
    subir_gcs_desde_memoria(buffer, nombre_archivo)

if __name__ == "__main__":
    main()
