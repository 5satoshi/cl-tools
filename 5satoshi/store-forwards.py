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

        status_query = """
            SELECT status,
                   min(received_time) as mintime,
                   max(received_time) as maxtime,
                   min(created_index) as minindex,
                   max(created_index) as maxindex
            FROM `lightning-fee-optimizer.version_1.forwardings`
            GROUP BY status
        """
        status_result = client.query(status_query).to_dataframe()
        logger.info(f"Status query returned {len(status_result)} rows.")

        # Log max created_index per status
        if not status_result.empty:
            for _, row in status_result.iterrows():
                status = row['status']
                max_idx = row['maxindex']
                logger.info(f"Status '{status}': max created_index = {max_idx}")
        else:
            logger.info("No existing forwardings found in BigQuery.")

        offered_minindex = status_result.minindex[
            status_result.status == 'offered'
        ]

        if offered_minindex.empty:
            index_start = status_result.maxindex.max() + 1
            logger.info(f"No 'offered' records found. Starting from index {index_start}")
        else:
            index_start = offered_minindex.iloc[0]
            logger.info(f"Found unfinished 'offered' record. Re-syncing from index {index_start}")

            if not DRY_RUN:
                del_query = """
                    DELETE FROM `lightning-fee-optimizer.version_1.forwardings`
                    WHERE created_index >= {}
                """.format(index_start)
                client.query(del_query).result()
                logger.info("Deleted overlapping forwardings from BigQuery.")
            else:
                logger.info("DRY RUN: Skipping DELETE query.")

    except Exception:
        logger.exception("Failed during BigQuery status check.")
        sys.exit(1)

    # -------------------------------------------------
    # Fetch Forwardings from Lightning
    # -------------------------------------------------
    try:
        logger.info(f"Fetching forwards from Lightning starting at index {index_start}...")

        forwards = l1.listforwards(index='created', start=int(index_start))
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

        # Integer columns (nullable)
        dff["created_index"] = dff["created_index"].astype("Int64")
        dff["in_msat"] = dff["in_msat"].astype("Int64")

        # Float columns
        float_cols = [
            "out_msat", "fee_msat",
            "in_htlc_id", "failcode",
            "out_htlc_id", "updated_index"
        ]
        for col in float_cols:
            dff[col] = dff[col].astype("float64")

        # String columns
        string_cols = [
            "in_channel", "out_channel",
            "status", "failreason", "style"
        ]
        for col in string_cols:
            dff[col] = dff[col].astype("string")


        # -------------------------
        # Safe Timestamp Conversion (ns -> us)
        # -------------------------
        for col in ["received_time", "resolved_time"]:
            # Convert to datetime (if not already)
            dff[col] = pd.to_datetime(dff[col], errors="coerce", utc=True)
            # Convert nanoseconds to microseconds safely
            dff[col] = dff[col].apply(lambda x: int(x.value // 1000) if pd.notnull(x) else pd.NaT)
            # Back to datetime (microseconds, UTC)
            dff[col] = pd.to_datetime(dff[col], unit='us', utc=True)

        # Timestamp conversion
        #dff["received_time"] = pd.to_datetime(dff["received_time"], unit="s", errors="coerce", utc="True")
        #dff["resolved_time"] = pd.to_datetime(dff["resolved_time"], unit="s", errors="coerce", utc="True")

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
                write_disposition="WRITE_APPEND"
            )

            job = client.load_table_from_dataframe(
                dff,
                "lightning-fee-optimizer.version_1.forwardings",
                job_config=job_config
            )
            job.result()
            logger.info(f"Successfully uploaded {len(dff)} forward records.")

    except Exception:
        logger.exception("Failed during BigQuery upload.")
        sys.exit(1)

    logger.info("Forwardings sync completed successfully.")


# -------------------------------------------------
# Main Guard
# -------------------------------------------------
if __name__ == "__main__":
    main()

