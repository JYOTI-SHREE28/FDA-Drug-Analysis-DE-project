import requests
import pandas as pd
import time
import concurrent.futures


def fetch_event_data_last_4_months_2024(max_rows=1000):
    start_date = "20240901"  # September 1, 2024
    end_date = "20241231"    # December 31, 2024
    limit = 100              # max per API call
    skip = 0
    event_url = "https://api.fda.gov/drug/event.json"
    event_records = []

    print("📦 Fetching adverse event data for September to December 2024...")

    while len(event_records) < max_rows:
        query = f"receivedate:[{start_date}+TO+{end_date}]"
        url = f"{event_url}?search={query}&limit={limit}&skip={skip}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ Error fetching event data: {response.status_code} {response.text}")
            break
        data = response.json()
        records = data.get("results", [])
        if not records:
            break
        for record in records:
            patient = record.get("patient", {})
            age = patient.get("patientonsetage", "N/A")
            age_unit = patient.get("patientonsetageunit", "N/A")
            reactions = patient.get("reaction", [])
            reaction = reactions[0].get("reactionmeddrapt", "N/A") if reactions else "N/A"
            drugs = patient.get("drug", [])
            for drug in drugs:
                drug_name = drug.get("medicinalproduct", "N/A")
                event_records.append({
                    "Drug Name": drug_name,
                    "Patient Age": age,
                    "Age Unit": age_unit,
                    "Drug Reaction": reaction
                })
                if len(event_records) >= max_rows:
                    break
            if len(event_records) >= max_rows:
                break
        if len(records) < limit:
            break
        skip += limit
        time.sleep(0.5)  
    return event_records[:max_rows]

#fetch lael data
def fetch_label_data(drug_name):
    label_url = "https://api.fda.gov/drug/label.json"
    params = {"search": f"openfda.brand_name:{drug_name}", "limit": 1}
    try:
        response = requests.get(label_url, params=params)
        if response.status_code != 200:
            return None
        data = response.json()
        results = data.get("results", [])
        if not results:
            return None
        label = results[0]
        dosage = label.get("dosage_and_administration", ["N/A"])[0]
        indications_usage = label.get("indications_and_usage", ["N/A"])[0]
        overdose_info = label.get("overdosage", ["N/A"])[0] if "overdosage" in label else label.get("warnings", ["N/A"])[0]
        manufacturer = label.get("openfda", {}).get("manufacturer_name", ["N/A"])[0]
        generic_name = label.get("openfda", {}).get("generic_name", ["N/A"])[0]
        return {
            "Dosage": dosage,
            "Indications & Usage": indications_usage,
            "Overdose Side Effects": overdose_info,
            "Manufacturer": manufacturer,
            "Generic Name": generic_name
        }
    except Exception as e:
        print(f"⚠️ Error fetching label data for {drug_name}: {e}")
        return None

#concurrent label fetch
def fetch_all_label_data(unique_drugs, max_workers=5):
    label_info_mapping = {}
    print("🔎 Fetching labeling details concurrently for unique drugs...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_drug = {
            executor.submit(fetch_label_data, drug): drug 
            for drug in unique_drugs if drug != "N/A"
        }
        for future in concurrent.futures.as_completed(future_to_drug):
            drug = future_to_drug[future]
            try:
                label_info = future.result()
            except Exception as exc:
                print(f"⚠️ Error fetching data for {drug}: {exc}")
                label_info = None
            if label_info:
                label_info_mapping[drug] = label_info
            else:
                label_info_mapping[drug] = {
                    "Dosage": "N/A", 
                    "Indications & Usage": "N/A", 
                    "Overdose Side Effects": "N/A",
                    "Manufacturer": "N/A",
                    "Generic Name": "N/A"
                }
    return label_info_mapping

# merge event + label data 
def merge_data(event_records):
    df_events = pd.DataFrame(event_records)
    unique_drugs = df_events["Drug Name"].unique()
    label_info_mapping = fetch_all_label_data(unique_drugs, max_workers=5)

    def get_label_value(drug, key):
        return label_info_mapping.get(drug, {}).get(key, "N/A")

    df_events["Dosage"] = df_events["Drug Name"].apply(lambda x: get_label_value(x, "Dosage"))
    df_events["Indications & Usage"] = df_events["Drug Name"].apply(lambda x: get_label_value(x, "Indications & Usage"))
    df_events["Overdose Side Effects"] = df_events["Drug Name"].apply(lambda x: get_label_value(x, "Overdose Side Effects"))
    df_events["Manufacturer"] = df_events["Drug Name"].apply(lambda x: get_label_value(x, "Manufacturer"))
    df_events["Generic Name"] = df_events["Drug Name"].apply(lambda x: get_label_value(x, "Generic Name"))

    return df_events


event_records = fetch_event_data_last_4_months_2024(max_rows=1000)

if not event_records:
    print("⚠️ No event data found for the last 4 months of 2024.")
else:
    final_df = merge_data(event_records)

    print("✅ Shape after merging:", final_df.shape)
    print("🧾 Columns:", final_df.columns.tolist())

    final_df.to_csv("fda_drug_event.csv", index=False)
    print("📁 Data merged and saved to 'fda_drug_event.csv'")
    print(final_df.head())