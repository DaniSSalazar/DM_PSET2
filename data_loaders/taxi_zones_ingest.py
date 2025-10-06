if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import os
import requests
import csv
import snowflake.connector
import time
from mage_ai.data_preparation.shared.secrets import get_secret_value

TAXI_ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
LOCAL_CSV = "data/taxi_zones.csv"


def need(name, val):
    if not val:
        raise Exception(f"Falta el secret '{name}' en Mage (Settings → Secrets).")
    return val


# === Funciones de ayuda con reintentos ===
def retry_request(url, retries=3, delay=5):
    """
    Descarga un archivo con reintentos exponenciales.
    """
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"⚠️ Error en intento {i}/{retries} al descargar {url}: {e}")
            if i < retries:
                time.sleep(delay * i)  # backoff exponencial
            else:
                raise


def retry_snowflake_op(cur, sql, params=None, retries=3, delay=5):
    """
    Ejecuta una query Snowflake con reintentos si hay error temporal.
    """
    for i in range(1, retries + 1):
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return
        except Exception as e:
            print(f"⚠️ Error Snowflake intento {i}/{retries}: {e}")
            if i < retries:
                time.sleep(delay * i)
            else:
                raise


@data_loader
def load_taxi_zones(*args, **kwargs):
    """
    Descarga el CSV oficial de taxi zones y lo inserta en BRONZE.TAXI_ZONES.
    Idempotente + reintentos + inserción en lotes.
    """
    # ---------- credenciales ----------
    sf_user      = need("SNOWFLAKE_USER",      get_secret_value("SNOWFLAKE_USER"))
    sf_password  = need("SNOWFLAKE_PASSWORD",  get_secret_value("SNOWFLAKE_PASSWORD"))
    sf_account   = need("SNOWFLAKE_ACCOUNT",   get_secret_value("SNOWFLAKE_ACCOUNT"))
    sf_warehouse = get_secret_value("SNOWFLAKE_WAREHOUSE") or "WH_INGEST"
    sf_database  = get_secret_value("SNOWFLAKE_DATABASE")  or "NYC_TAXI"
    sf_schema    = 'SILVER'

    # ---------- conexión Snowflake ----------
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        role="SYSADMIN",
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        insecure_mode=True  # quítalo si ya resolviste certificados
    )
    cur = conn.cursor()

    # ---------- descarga del CSV con reintentos ----------
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(LOCAL_CSV):
        print(f"Descargando {TAXI_ZONES_URL} …")
        r = retry_request(TAXI_ZONES_URL, retries=5, delay=5)
        with open(LOCAL_CSV, "wb") as f:
            f.write(r.content)

    # ---------- crear tabla si no existe ----------
    retry_snowflake_op(cur, f"""
        CREATE TABLE IF NOT EXISTS {sf_database}.{sf_schema}.TAXI_ZONES (
            LOCATIONID    NUMBER(38,0),
            BOROUGH       STRING,
            ZONE          STRING,
            SERVICE_ZONE  STRING
        )
    """)

    # ---------- limpiar tabla para idempotencia ----------
    retry_snowflake_op(cur, f"TRUNCATE TABLE {sf_database}.{sf_schema}.TAXI_ZONES")

    # ---------- insertar datos en lotes ----------
    batch_size = 1000  # puedes cambiarlo si quieres
    batch = []
    inserted = 0

    with open(LOCAL_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            batch.append((
                int(row["LocationID"]) if row.get("LocationID") else None,
                row.get("Borough"),
                row.get("Zone"),
                row.get("service_zone"),
            ))

            if len(batch) >= batch_size:
                retry_snowflake_op(
                    cur,
                    f"INSERT INTO {sf_database}.{sf_schema}.TAXI_ZONES (LOCATIONID,BOROUGH,ZONE,SERVICE_ZONE) VALUES " +
                    ", ".join(["(%s,%s,%s,%s)"] * len(batch)),
                    [val for tup in batch for val in tup]
                )
                inserted += len(batch)
                batch = []

    # Inserta el último batch si quedó algo
    if batch:
        retry_snowflake_op(
            cur,
            f"INSERT INTO {sf_database}.{sf_schema}.TAXI_ZONES (LOCATIONID,BOROUGH,ZONE,SERVICE_ZONE) VALUES " +
            ", ".join(["(%s,%s,%s,%s)"] * len(batch)),
            [val for tup in batch for val in tup]
        )
        inserted += len(batch)

    cur.close()
    conn.close()

    print(f"✅ Cargadas {inserted} filas en {sf_database}.{sf_schema}.TAXI_ZONES (con reintentos y batch insert)")
    return {"rows_inserted": inserted}
