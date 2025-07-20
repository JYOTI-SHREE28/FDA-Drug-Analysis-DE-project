import os
import time
from dotenv import load_dotenv
from extract import fetch_event_data_last_4_months_2024
from transform import merge_data
from load import load_data_to_mysql

# Load environment variables from config.env
load_dotenv("config.env")

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def main():
    log("🚀 Starting ETL Pipeline...")

    max_rows = int(os.getenv("MAX_ROWS", 1000))
    log(f"🔍 Extracting data (max rows: {max_rows})...")
    events = fetch_event_data_last_4_months_2024(max_rows=max_rows)

    if not events:
        log("⚠️ No event data found. Exiting.")
        return

    log("🧼 Transforming data...")
    transformed_df = merge_data(events)

    log("📥 Loading data into MySQL...")
    load_data_to_mysql(
        transformed_df,
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    log("✅ ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()