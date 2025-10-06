#data loader para los taxis verdes para que vayan a el esquema bronze en snowflake
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

import os, time, requests
import pyarrow.parquet as pq
import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def need(name, val):
    if not val:
        raise Exception(f"Falta el secret '{name}' en Mage (Settings → Secrets).")
    return val


def load_green_month_chunked(*, year:int, month:int, chunk_size:int=1_000_000, max_retries:int=3):
    service    = "green"
    dest_dir   = "data/nyc_tlc"
    stage_name = "TAXI_STAGE"
    table_name = "GREEN_TRIPS"             # tabla final de datos
    meta_table = "GREEN_TRIPS_METADATA"    # tabla de metadatos
    tmp_table  = "_TMP_RAW_VARIANT"        # staging temporal

    os.makedirs(dest_dir, exist_ok=True)
    fname = f"{service}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{fname}"
    local_path = os.path.join(dest_dir, fname)

    # -------- credenciales --------
    sf_user      = need("SNOWFLAKE_USER",      get_secret_value("SNOWFLAKE_USER"))
    sf_password  = need("SNOWFLAKE_PASSWORD",  get_secret_value("SNOWFLAKE_PASSWORD"))
    sf_account   = need("SNOWFLAKE_ACCOUNT",   get_secret_value("SNOWFLAKE_ACCOUNT"))
    sf_warehouse = get_secret_value("SNOWFLAKE_WAREHOUSE") or "WH_INGEST"
    sf_database  = get_secret_value("SNOWFLAKE_DATABASE")  or "NYC_TAXI"
    sf_schema    = get_secret_value("SNOWFLAKE_SCHEMA")    or "RAW"

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        role="SYSADMIN",
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema,
        insecure_mode=True
    )
    cur = conn.cursor()

    # -------- objetos base --------
    cur.execute(f"CREATE FILE FORMAT IF NOT EXISTS {sf_database}.{sf_schema}.PARQUET_FORMAT TYPE=PARQUET")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {sf_database}.{sf_schema}.{stage_name} "
                f"FILE_FORMAT={sf_database}.{sf_schema}.PARQUET_FORMAT")

    # -------- descarga parquet --------
    if not os.path.exists(local_path):
        print(f"Descargando {url} …")
        r = requests.get(url, timeout=180)
        if r.status_code == 404:
            run_id = f"{service}_{year}{month:02d}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            cur.execute(f"""
                INSERT INTO {sf_database}.{sf_schema}.{meta_table}
                (RUN_ID,SERVICE,YEAR,MONTH,SOURCE_FILE,STATUS,ERROR_MESSAGE,INGEST_TS)
                VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())
            """, (run_id, service, year, month, fname, 'MISSING', 'HTTP 404'))
            cur.close(); conn.close()
            print(f"⚠️ Archivo no encontrado: {url}. Marcado MISSING en metadatos.")
            return {"file": fname, "status": "MISSING"}
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            f.write(r.content)

    pf = pq.ParquetFile(local_path)
    rows_in_file = pf.metadata.num_rows

    # -------- subir al stage --------
    print(f"Subiendo {fname} al stage…")
    cur.execute(f"PUT file://{os.path.abspath(local_path)} "
                f"@{sf_database}.{sf_schema}.{stage_name} OVERWRITE=TRUE")

    # -------- staging temporal --------
    print("Creando tabla temporal staging …")
    cur.execute(f"CREATE OR REPLACE TEMP TABLE {tmp_table} (V VARIANT)")

    print("COPY INTO staging VARIANT …")
    cur.execute(f"""
        COPY INTO {tmp_table}(V)
        FROM @{sf_database}.{sf_schema}.{stage_name}/{fname}
        FILE_FORMAT = (TYPE=PARQUET)
        ON_ERROR = ABORT_STATEMENT
    """)

    # -------- idempotencia --------
    print("Eliminando datos previos de", fname)
    cur.execute(f"DELETE FROM {sf_database}.{sf_schema}.{table_name} WHERE SOURCE_FILE = %s", (fname,))

    # -------- chunking con reintentos --------
    total_inserted = 0
    n_chunks = (rows_in_file + chunk_size - 1) // chunk_size
    run_id_base = f"{service}_{year}{month:02d}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    for chunk_index in range(1, n_chunks + 1):
        start_rn = (chunk_index - 1) * chunk_size + 1
        end_rn   = min(chunk_index * chunk_size, rows_in_file)
        run_id   = f"{run_id_base}_c{chunk_index}"
        print(f"Chunk {chunk_index}/{n_chunks}: rn {start_rn}-{end_rn}")

        insert_sql = f"""
        INSERT INTO {sf_database}.{sf_schema}.{table_name}
        (VENDORID, LPEP_PICKUP_DATETIME, LPEP_DROPOFF_DATETIME, STORE_AND_FWD_FLAG,
         RATECODEID, PULOCATIONID, DOLOCATIONID, PASSENGER_COUNT, TRIP_DISTANCE,
         FARE_AMOUNT, EXTRA, MTA_TAX, TIP_AMOUNT, TOLLS_AMOUNT, EHAIL_FEE,
         IMPROVEMENT_SURCHARGE, TOTAL_AMOUNT, PAYMENT_TYPE, TRIP_TYPE,
         CONGESTION_SURCHARGE, CBD_CONGESTION_FEE, SOURCE_FILE)
        SELECT
            TRY_TO_NUMBER(v:VendorID::string),
            TO_TIMESTAMP_NTZ(v:lpep_pickup_datetime::string),
            TO_TIMESTAMP_NTZ(v:lpep_dropoff_datetime::string),
            v:store_and_fwd_flag::string,
            TRY_TO_NUMBER(v:RatecodeID::string),
            TRY_TO_NUMBER(v:PULocationID::string),
            TRY_TO_NUMBER(v:DOLocationID::string),
            TRY_TO_NUMBER(v:passenger_count::string),
            TRY_TO_DECIMAL(v:trip_distance::string, 12, 3),
            TRY_TO_DECIMAL(v:fare_amount::string, 12, 2),
            TRY_TO_DECIMAL(v:extra::string, 12, 2),
            TRY_TO_DECIMAL(v:mta_tax::string, 12, 2),
            TRY_TO_DECIMAL(v:tip_amount::string, 12, 2),
            TRY_TO_DECIMAL(v:tolls_amount::string, 12, 2),
            TRY_TO_DECIMAL(v:ehail_fee::string, 12, 2),
            TRY_TO_DECIMAL(v:improvement_surcharge::string, 12, 2),
            TRY_TO_DECIMAL(v:total_amount::string, 12, 2),
            TRY_TO_NUMBER(v:payment_type::string),
            TRY_TO_NUMBER(v:trip_type::string),
            TRY_TO_DECIMAL(v:congestion_surcharge::string, 12, 2),
            TRY_TO_DECIMAL(v:cbd_congestion_fee::string, 12, 2),
            '{fname}'
        FROM (
            SELECT V, ROW_NUMBER() OVER (ORDER BY V:lpep_pickup_datetime::string) AS rn
            FROM {tmp_table}
        )
        WHERE rn BETWEEN {start_rn} AND {end_rn}
        """

        attempt = 0
        success = False
        while attempt < max_retries and not success:
            try:
                cur.execute(insert_sql)
                inserted_chunk = cur.rowcount
                total_inserted += inserted_chunk

                cur.execute(f"""
                    INSERT INTO {sf_database}.{sf_schema}.{meta_table}
                    (RUN_ID,SERVICE,YEAR,MONTH,SOURCE_FILE,CHUNK_INDEX,CHUNK_SIZE,
                     ROWS_IN_FILE,ROWS_INSERTED,STATUS,ERROR_MESSAGE,INGEST_TS)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())
                """, (run_id, service, year, month, fname, chunk_index, chunk_size,
                      rows_in_file, inserted_chunk, 'OK', None))
                success = True
            except Exception as e:
                attempt += 1
                if attempt >= max_retries:
                    cur.execute(f"""
                        INSERT INTO {sf_database}.{sf_schema}.{meta_table}
                        (RUN_ID,SERVICE,YEAR,MONTH,SOURCE_FILE,CHUNK_INDEX,CHUNK_SIZE,
                         ROWS_IN_FILE,ROWS_INSERTED,STATUS,ERROR_MESSAGE,INGEST_TS)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())
                    """, (run_id, service, year, month, fname, chunk_index, chunk_size,
                          rows_in_file, 0, 'ERROR', str(e)))
                    raise
                else:
                    print(f"⚠️ Error en chunk {chunk_index} intento {attempt}: {e}, reintentando…")
                    time.sleep(5)

    cur.close()
    conn.close()

    print(f"✅ {year}-{month:02d} cargado: {total_inserted}/{rows_in_file}")
    return {
        "file": fname,
        "year": year,
        "month": month,
        "chunk_size": chunk_size,
        "rows_in_file": rows_in_file,
        "rows_inserted": total_inserted
    }


@data_loader
def backfill_green_all_months(*args, **kwargs):
    """
    Backfill completo de Green Taxi 2015-01 a 2025-12.
    Idempotente y con reintentos en cada chunk.
    """
    start_year, start_month = 2015, 1
    end_year, end_month = 2025, 12

    months = []
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

    results = []
    for (y, m) in months:
        print(f"\n=== Procesando {y}-{m:02d} ===")
        try:
            res = load_green_month_chunked(year=y, month=m, chunk_size=1_000_000, max_retries=3)
            results.append({"year": y, "month": m, "status": "OK",
                            "rows_inserted": res.get("rows_inserted"),
                            "rows_in_file": res.get("rows_in_file")})
        except Exception as e:
            print(f"⚠️ Error en {y}-{m:02d}: {e}")
            results.append({"year": y, "month": m, "status": "ERROR", "error": str(e)})

    print("\n✅ Backfill terminado")
    return results
