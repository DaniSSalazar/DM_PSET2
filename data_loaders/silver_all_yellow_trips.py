if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import os, time, requests
import pyarrow.parquet as pq
import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def need(name, val):
    if not val:
        raise Exception(f"Falta el secret '{name}' en Mage (Settings → Secrets).")
    return val

def load_yellow_to_silver(*, year:int, month:int, chunk_size:int=1_000_000, max_retries:int=3):
    """
    Carga un mes de Yellow Taxi a SILVER.TAXI_TRIPS_ALL con limpieza y estandarización.
    """
    service    = "yellow"
    dest_dir   = "data/nyc_tlc"
    stage_name = "TAXI_STAGE"
    tmp_table  = "_TMP_RAW_VARIANT"

    os.makedirs(dest_dir, exist_ok=True)
    fname = f"{service}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{fname}"
    local_path = os.path.join(dest_dir, fname)

    # --- credenciales ---
    sf_user      = need("SNOWFLAKE_USER",      get_secret_value("SNOWFLAKE_USER"))
    sf_password  = need("SNOWFLAKE_PASSWORD",  get_secret_value("SNOWFLAKE_PASSWORD"))
    sf_account   = need("SNOWFLAKE_ACCOUNT",   get_secret_value("SNOWFLAKE_ACCOUNT"))
    sf_warehouse = get_secret_value("SNOWFLAKE_WAREHOUSE") or "WH_INGEST"
    sf_database  = get_secret_value("SNOWFLAKE_DATABASE")  or "NYC_TAXI"

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        role="SYSADMIN",
        warehouse=sf_warehouse,
        database=sf_database,
        schema="SILVER",
        insecure_mode = True
    )
    cur = conn.cursor()

    # --- objetos base ---
    cur.execute(f"CREATE FILE FORMAT IF NOT EXISTS {sf_database}.SILVER.PARQUET_FORMAT TYPE=PARQUET")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {sf_database}.SILVER.{stage_name} "
                f"FILE_FORMAT={sf_database}.SILVER.PARQUET_FORMAT")

    # --- descarga parquet ---
    if not os.path.exists(local_path):
        print(f"Descargando {url} …")
        r = requests.get(url, timeout=180)
        if r.status_code == 404:
            print(f"⚠️ Archivo no encontrado: {url}")
            return {"file": fname, "status": "MISSING"}
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(r.content)

    pf = pq.ParquetFile(local_path)
    rows_in_file = pf.metadata.num_rows
    print(f"Archivo {fname} con {rows_in_file} filas")

    # --- subir al stage ---
    cur.execute(f"PUT file://{os.path.abspath(local_path)} "
                f"@{sf_database}.SILVER.{stage_name} OVERWRITE=TRUE")

    # --- staging temporal ---
    cur.execute(f"CREATE OR REPLACE TEMP TABLE {tmp_table} (V VARIANT)")
    cur.execute(f"""
        COPY INTO {tmp_table}(V)
        FROM @{sf_database}.SILVER.{stage_name}/{fname}
        FILE_FORMAT = (TYPE=PARQUET)
        ON_ERROR = ABORT_STATEMENT
    """)

    # --- idempotencia ---
    cur.execute(f"DELETE FROM {sf_database}.SILVER.TAXI_TRIPS_ALL WHERE SOURCE_FILE = %s", (fname,))

    # --- chunking ---
    total_inserted = 0
    n_chunks = (rows_in_file + chunk_size - 1) // chunk_size

    for chunk_index in range(1, n_chunks + 1):
        start_rn = (chunk_index - 1) * chunk_size + 1
        end_rn   = min(chunk_index * chunk_size, rows_in_file)
        print(f"Chunk {chunk_index}/{n_chunks}: filas {start_rn}-{end_rn}")

        insert_sql = f"""
            INSERT INTO {sf_database}.SILVER.TAXI_TRIPS_ALL
            (AIRPORT_FEE, CBD_CONGESTION_FEE, CONGESTION_SURCHARGE,
            DOLOCATION_ID, DROPOFF_DATETIME, EHAIL_FEE, EXTRA, FARE_AMOUNT,
            IMPROVEMENT_SURCHARGE, LOAD_TS, MTA_TAX, PASSENGER_COUNT,
            PAYMENT_TYPE, PAYMENT_TYPE_ID, PICKUP_DATETIME, PULOCATION_ID,
            RATECODE_ID, SERVICE_TYPE, SOURCE_FILE, STORE_AND_FWD_FLAG,
            TIP_AMOUNT, TOLLS_AMOUNT, TOTAL_AMOUNT, TRIP_DISTANCE, TRIP_TYPE,
            VENDOR_ID)
            SELECT
                TRY_TO_DECIMAL(v:Airport_fee::string, 12, 2)                         AS AIRPORT_FEE,
                TRY_TO_DECIMAL(v:cbd_congestion_fee::string, 12, 2)                  AS CBD_CONGESTION_FEE,
                TRY_TO_DECIMAL(v:congestion_surcharge::string, 12, 2)                AS CONGESTION_SURCHARGE,
                TRY_TO_NUMBER(v:DOLocationID::string)                                AS DOLOCATION_ID,
                TO_TIMESTAMP_NTZ(v:tpep_dropoff_datetime::string)                    AS DROPOFF_DATETIME,
                NULL                                                                 AS EHAIL_FEE,
                TRY_TO_DECIMAL(v:extra::string, 12, 2)                               AS EXTRA,
                TRY_TO_DECIMAL(v:fare_amount::string, 12, 2)                         AS FARE_AMOUNT,
                TRY_TO_DECIMAL(v:improvement_surcharge::string, 12, 2)               AS IMPROVEMENT_SURCHARGE,
                CURRENT_TIMESTAMP()                                                  AS LOAD_TS,
                TRY_TO_DECIMAL(v:mta_tax::string, 12, 2)                             AS MTA_TAX,
                NULLIF(TRY_TO_NUMBER(v:passenger_count::string),0)                   AS PASSENGER_COUNT,
                CASE TRY_TO_NUMBER(v:payment_type::string)
                    WHEN 1 THEN 'Credit Card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No Charge'
                    WHEN 4 THEN 'Dispute'
                    WHEN 5 THEN 'Unknown'
                    WHEN 6 THEN 'Voided Trip'
                    ELSE 'Other'
                END                                                                  AS PAYMENT_TYPE,
                TRY_TO_NUMBER(v:payment_type::string)                                AS PAYMENT_TYPE_ID,
                TO_TIMESTAMP_NTZ(v:tpep_pickup_datetime::string)                     AS PICKUP_DATETIME,
                TRY_TO_NUMBER(v:PULocationID::string)                                AS PULOCATION_ID,
                TRY_TO_NUMBER(v:RatecodeID::string)                                  AS RATECODE_ID,
                'yellow'                                                             AS SERVICE_TYPE,
                '{fname}'                                                            AS SOURCE_FILE,
                v:store_and_fwd_flag::string                                         AS STORE_AND_FWD_FLAG,
                TRY_TO_DECIMAL(v:tip_amount::string, 12, 2)                          AS TIP_AMOUNT,
                TRY_TO_DECIMAL(v:tolls_amount::string, 12, 2)                        AS TOLLS_AMOUNT,
                TRY_TO_DECIMAL(v:total_amount::string, 12, 2)                        AS TOTAL_AMOUNT,
                TRY_TO_DECIMAL(v:trip_distance::string, 12, 3)                       AS TRIP_DISTANCE,
                TRY_TO_NUMBER(v:trip_type::string)                                   AS TRIP_TYPE,
                TRY_TO_NUMBER(v:VendorID::string)                                    AS VENDOR_ID
            FROM (
                SELECT V, ROW_NUMBER() OVER (ORDER BY V:tpep_pickup_datetime::string) AS rn
                FROM {tmp_table}
            )
            WHERE rn BETWEEN {start_rn} AND {end_rn}
            -- Reglas de calidad mínimas:
            AND v:tpep_pickup_datetime IS NOT NULL
            AND v:tpep_dropoff_datetime IS NOT NULL
            AND TRY_TO_DECIMAL(v:trip_distance::string,12,3) >= 0
            AND TRY_TO_DECIMAL(v:total_amount::string,12,2) >= 0
            AND DATEDIFF('hour', TO_TIMESTAMP_NTZ(v:tpep_pickup_datetime::string),
                                TO_TIMESTAMP_NTZ(v:tpep_dropoff_datetime::string)) <= 24
            """


        attempt = 0
        while attempt < max_retries:
            try:
                cur.execute(insert_sql)
                total_inserted += cur.rowcount
                break
            except Exception as e:
                attempt += 1
                if attempt == max_retries:
                    print(f"❌ Error definitivo en chunk {chunk_index}: {e}")
                    raise
                else:
                    print(f"⚠️ Error en chunk {chunk_index} intento {attempt}: {e}, reintentando…")
                    time.sleep(5)

    cur.close()
    conn.close()
    print(f"✅ {year}-{month:02d}: {total_inserted}/{rows_in_file} filas insertadas")
    return {"file": fname, "rows_inserted": total_inserted, "rows_in_file": rows_in_file}

@data_loader
def backfill_yellow_silver_all_months(*args, **kwargs):
    """
    Backfill completo Yellow Taxi → SILVER.TAXI_TRIPS_ALL (2015-01 → 2025-12)
    """
    start_year, start_month = 2015, 1
    end_year, end_month = 2025, 12
    y, m = start_year, start_month
    results = []

    while (y < end_year) or (y == end_year and m <= end_month):
        print(f"\n=== Procesando {y}-{m:02d} ===")
        try:
            res = load_yellow_to_silver(year=y, month=m)
            results.append({"year": y, "month": m, "status": "OK", **res})
        except Exception as e:
            results.append({"year": y, "month": m, "status": "ERROR", "error": str(e)})
        if m == 12:
            y += 1; m = 1
        else:
            m += 1
    print("\n✅ Backfill terminado")
    return results
