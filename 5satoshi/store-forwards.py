import argparse
from pyln.client import LightningRpc
import pandas as pd
import sys, os, logging
from google.cloud import bigquery


def main():
    # -------------------------------------------------
    # CLI Argument Parsing
    # -------------------------------------------------
    parser = argparse.ArgumentParser(description="Sync Lightning forwardings to BigQuery")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run script without uploading to BigQuery (dry run)"
    )
    args = parser.parse_args()
    DRY_RUN = args.test

    # -------------------------------------------------
    # Logging Configuration
    # -------------------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)

    logger.info("Starting forwardings sync script.")
    if DRY_RUN:
        logger.info("DRY RUN MODE ENABLED: No data will be uploaded to BigQuery.")

    # -------------------------------------------------
    # Initialize Clients
    # -------------------------------------------------
    try:
        logger.info("Initializing BigQuery client...")
        client = bigquery.Client()
        logger.info("BigQuery client initialized.")

        logger.info("Initializing Lightning RPC client...")
        rpc_path = os.environ['HOME'] + "/.lightning/bitcoin/lightning-rpc"
        l1 = LightningRpc(rpc_path)
        logger.info(f"Lightning RPC connected at {rpc_path}")

    except Exception:
        logger.exception("Failed to initialize clients.")
        sys.exit(1)

    # -------------------------------------------------
    # Forwardings Sync
    # -------------------------------------------------
    try:
        logger.info("Querying existing forwardings status from BigQuery...")
        
        # Fetch max indexes from BigQuery
        result = client.query("""
            SELECT MAX(updated_index) AS max_updated
            FROM `lightning-fee-optimizer.version_1.forwardings`
        """).to_dataframe()
        max_updated = result["max_updated"].iloc[0]
        if pd.isna(max_updated):
            max_updated = 0

    except Exception:
        logger.exception("Failed during BigQuery status check.")
        sys.exit(1)

    # -------------------------------------------------
    # Fetch Forwardings from Lightning
    # -------------------------------------------------
    try:
        start_index = int(max_updated) + 1
        logger.info(f"Fetching forwards from Lightning starting at updated_index {start_index}...")
        forwards = l1.listforwards(index='updated', start=start_index)
        dff = pd.DataFrame(forwards["forwards"])
        logger.info(f"Fetched {len(dff)} forward records.")

        if dff.empty:
            logger.info("No new forwardings to process. Exiting.")
            sys.exit(0)

    except Exception:
        logger.exception("Failed while fetching forwardings from Lightning.")
        sys.exit(1)

    # -------------------------------------------------
    # Data Cleaning & Type Enforcement
    # -------------------------------------------------
    try:
        logger.info("Enforcing schema and data types...")

        expected_columns = [
            "created_index", "in_channel", "out_channel",
            "in_msat", "out_msat", "fee_msat",
            "status", "received_time", "resolved_time",
            "in_htlc_id", "failcode", "failreason",
            "out_htlc_id", "style", "updated_index"
        ]

        for col in expected_columns:
            if col not in dff:
                dff[col] = None

        dff = dff[expected_columns]

        # Float columns (nullable)
        dff["out_msat"] = dff["out_msat"].astype("Float64")
        dff["fee_msat"] = dff["fee_msat"].astype("Float64")

        # Int columns
        int_cols = [
            "in_htlc_id", "failcode",
            "out_htlc_id", "updated_index",
            "in_msat","created_index"
        ]
        for col in int_cols:
            dff[col] = dff[col].astype("Int64")

        # String columns
        string_cols = [
            "in_channel", "out_channel",
            "status", "failreason", "style"
        ]
        for col in string_cols:
            dff[col] = dff[col].astype("string")

        # Timestamp conversion
        dff["received_time"] = pd.to_datetime(dff["received_time"], unit="s", errors="coerce", utc=True).dt.round("us")
        dff["resolved_time"] = pd.to_datetime(dff["resolved_time"], unit="s", errors="coerce", utc=True).dt.round("us")

        logger.info("Schema enforcement complete.")

    except Exception:
        logger.exception("Failed during dtype enforcement.")
        sys.exit(1)

    # -------------------------------------------------
    # Upload to BigQuery
    # -------------------------------------------------
    try:
        if DRY_RUN:
            logger.info(f"DRY RUN: Skipping upload of {len(dff)} records to BigQuery.")
        else:
            logger.info(f"Uploading {len(dff)} forwardings to BigQuery...")

            schema = [
                bigquery.SchemaField("created_index", "INTEGER"),
                bigquery.SchemaField("in_channel", "STRING"),
                bigquery.SchemaField("out_channel", "STRING"),
                bigquery.SchemaField("in_msat", "INTEGER"),
                bigquery.SchemaField("out_msat", "FLOAT"),
                bigquery.SchemaField("fee_msat", "FLOAT"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("received_time", "TIMESTAMP"),
                bigquery.SchemaField("resolved_time", "TIMESTAMP"),
                bigquery.SchemaField("in_htlc_id", "FLOAT"),
                bigquery.SchemaField("failcode", "FLOAT"),
                bigquery.SchemaField("failreason", "STRING"),
                bigquery.SchemaField("out_htlc_id", "FLOAT"),
                bigquery.SchemaField("style", "STRING"),
                bigquery.SchemaField("updated_index", "FLOAT"),
            ]

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )

            job = client.load_table_from_dataframe(
                dff,
                "lightning-fee-optimizer.version_1.temp_forwardings",
                job_config=job_config
            )
            job.result()
            logger.info(f"Successfully uploaded {len(dff)} forward records.")

    except Exception:
        logger.exception("Failed during BigQuery upload.")
        sys.exit(1)

    logger.info("Forwardings sync completed successfully.")

    # -------------------------------------------------
    # Merge data
    # -------------------------------------------------
    try:
        if DRY_RUN:
            logger.info(f"DRY RUN: Skipping merge in BigQuery.")
        else:
            logger.info("Running MERGE into forwardings table...")
            
            # Fetch max indexes from BigQuery
            job = client.query("""
                MERGE `lightning-fee-optimizer.version_1.forwardings` T
                USING `lightning-fee-optimizer.version_1.temp_forwardings` S
                ON T.created_index = S.created_index
                WHEN MATCHED AND S.updated_index > T.updated_index THEN
                UPDATE SET
                    in_channel = S.in_channel,
                    out_channel = S.out_channel,
                    in_msat = S.in_msat,
                    out_msat = S.out_msat,
                    fee_msat = S.fee_msat,
                    status = S.status,
                    received_time = S.received_time,
                    resolved_time = S.resolved_time,
                    failcode = S.failcode,
                    failreason = S.failreason,
                    style = S.style,
                    updated_index = S.updated_index
                WHEN NOT MATCHED THEN
                INSERT ROW;
            """)
            job.result()

    except Exception:
        logger.exception("Failed during BigQuery merge")
        sys.exit(1)

# -------------------------------------------------
# Main Guard
# -------------------------------------------------
if __name__ == "__main__":
    main()

