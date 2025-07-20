import mysql.connector
import pandas as pd
from transform import merge_data
from extract import fetch_event_data_last_4_months_2024
from dotenv import load_dotenv
import os

# Load environment variables from config.env
load_dotenv("config.env")

def load_data_to_mysql(df, host, user, password, database, table_name="drug_events"):
    try:
        print("📋 Columns in DataFrame:", df.columns.tolist())

        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # 🚨 Drop the table if it exists (to avoid schema mismatch)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Recreate the table with cleaned column names
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            drug_name VARCHAR(255),
            patient_age VARCHAR(50),
            age_unit VARCHAR(50),
            drug_reaction TEXT,
            patient_age_numeric FLOAT,
            drug_name_cleaned VARCHAR(255),
            reaction_severity VARCHAR(50),
            age_group VARCHAR(50)
        );
        """
        cursor.execute(create_table_query)

        columns_to_insert = [
            "drug_name", "patient_age", "age_unit", "drug_reaction",
            "patient_age_numeric", "drug_name_cleaned",
            "reaction_severity", "age_group"
        ]

        print("✅ Final DataFrame columns:", df.columns.tolist())
        print("🔍 Sample row to insert:", df[columns_to_insert].iloc[0])

        for _, row in df[columns_to_insert].iterrows():
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns_to_insert)})
            VALUES ({', '.join(['%s'] * len(columns_to_insert))});
            """
            cursor.execute(insert_query, tuple(None if pd.isna(val) else val for val in row))

        connection.commit()
        print(f"✅ Data loaded into MySQL table '{table_name}' successfully.")

    except mysql.connector.Error as err:
        print(f"❌ Error: {err}")
    finally:
        cursor.close()
        connection.close()


# -------------------- RUN LOAD STEP --------------------
if __name__ == "__main__":
    events = fetch_event_data_last_4_months_2024(max_rows=int(os.getenv("MAX_ROWS", 1000)))
    if events:
        final_df = merge_data(events)
        load_data_to_mysql(
            final_df,
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
    else:
        print("⚠️ No events to load.")