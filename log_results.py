# ================================================================
# log_results.py
# Purpose: Log validation metadata into Snowflake (EDL_VALIDATION_LOG)
# ================================================================

import os
import argparse
from datetime import datetime
import snowflake.connector

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def main():
    # ------------------------------------------------------------
    # Parse arguments
    # ------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Log validation results to Snowflake")
    parser.add_argument("--file", required=False, default="Unknown")
    parser.add_argument("--status", required=False, default="FAIL")
    parser.add_argument("--domain", required=False, default="Unknown")
    parser.add_argument("--stage", required=False, default="Unknown")
    parser.add_argument("--source", required=False, default="Unknown")
    parser.add_argument("--rowcount", required=False)
    parser.add_argument("--colcount", required=False)
    parser.add_argument("--nullpct", required=False)
    parser.add_argument("--missingkeys", required=False)
    parser.add_argument("--errormsg", required=False)
    args = parser.parse_args()

    # ------------------------------------------------------------
    # Snowflake connection variables
    # ------------------------------------------------------------
    sf_account   = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user      = os.getenv("SNOWFLAKE_USER")
    sf_password  = os.getenv("SNOWFLAKE_PASSWORD")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    sf_database  = os.getenv("SNOWFLAKE_DATABASE")
    sf_schema    = os.getenv("SNOWFLAKE_SCHEMA")

    if not all([sf_account, sf_user, sf_password, sf_warehouse, sf_database, sf_schema]):
        print("⚠️ Snowflake environment variables missing. Skipping logging.")
        return

    # ------------------------------------------------------------
    # Prepare values
    # ------------------------------------------------------------
    file_name = os.path.basename(args.file) if args.file else "Unknown"

    values = (
        file_name,
        args.file or "Unknown",
        args.stage,
        args.status.upper(),
        args.domain,
        args.source,
        safe_float(args.rowcount),
        safe_float(args.colcount),
        safe_float(args.nullpct),
        args.missingkeys if args.missingkeys else None,
        args.errormsg if args.errormsg else None,
        "ADO_PIPELINE",
        datetime.utcnow()
    )

    # ------------------------------------------------------------
    # Insert SQL
    # ------------------------------------------------------------
    insert_sql = """
        INSERT INTO EDL_LOGS_DB.GOVERNANCE.VALIDATION_LOG
        (
            FILE_NAME,
            FILE_PATH,
            STAGE_NAME,
            VALIDATION_STATUS,
            DOMAIN,
            SOURCE,
            ROW_COUNT,
            COLUMN_COUNT,
            NULL_PCT,
            MISSING_KEYS,
            ERROR_MESSAGE,
            TRIGGERED_BY,
            LOG_TS
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        # ------------------------------------------------------------
        # Connect (NO CONTEXT HERE)
        # ------------------------------------------------------------
        conn = snowflake.connector.connect(
            account=sf_account,
            user=sf_user,
            password=sf_password
        )

        cur = conn.cursor()

        # ------------------------------------------------------------
        # ✅ EXPLICIT CONTEXT (CRITICAL FIX)
        # ------------------------------------------------------------
        cur.execute(f"USE WAREHOUSE {sf_warehouse}")
        cur.execute(f"USE DATABASE {sf_database}")
        cur.execute(f"USE SCHEMA {sf_schema}")

        # ------------------------------------------------------------
        # Insert
        # ------------------------------------------------------------
        cur.execute(insert_sql, values)
        conn.commit()

        print(f"✅ Logged {args.stage} | {args.status.upper()} | {file_name}")

    except Exception as e:
        print(f"❌ Snowflake logging failed: {e}")

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
