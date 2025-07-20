import pandas as pd
import numpy as np

def merge_data(event_records):
    
    df = pd.DataFrame(event_records)
    print("🔍 Raw data sample:")
    print(df.head(3))

    

    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    print("🧪 Columns present:", df.columns.tolist())

    
    df.replace(['N/A', 'NA', '', ' ', 'null', 'None', None], np.nan, inplace=True)
    print("📋 Nulls after initial cleaning:")
    print(df.isnull().sum())

    
    df.drop_duplicates(inplace=True)

    # ---------------- TRANSFORMATIONS ----------------

    
    df['patient_age_numeric'] = pd.to_numeric(df['patient_age'], errors='coerce')

    
    df['drug_name_cleaned'] = df['drug_name'].str.strip().str.lower()


    def extract_severity(reaction):
        if pd.isna(reaction):
            return 'unknown'
        reaction = reaction.lower()
        if any(word in reaction for word in ['death', 'fatal', 'severe', 'hospital']):
            return 'severe'
        elif any(word in reaction for word in ['rash', 'fever', 'nausea']):
            return 'moderate'
        else:
            return 'mild'

    df['reaction_severity'] = df['drug_reaction'].apply(extract_severity)

    
    def age_bucket(age):
        if pd.isna(age):
            return 'unknown'
        age = float(age)
        if age < 12:
            return 'child'
        elif age < 60:
            return 'adult'
        else:
            return 'senior'

    df['age_group'] = df['patient_age_numeric'].apply(age_bucket)

    

    
    essential_cols = ['drug_name', 'drug_reaction', 'age_group']
    before_drop = df.shape[0]
    df.dropna(subset=essential_cols, inplace=True)
    after_drop = df.shape[0]
    print(f"🧹 Dropped {before_drop - after_drop} rows due to nulls in {essential_cols}")

    
    print("✅ Final null counts:")
    print(df.isnull().sum())

    
    print("📦 Sample row ready for DB insert:")
    print(df.iloc[0])

    # Fill non-critical columns to avoid NULLs in MySQL
    df['patient_age'].fillna('0', inplace=True)  # or 'unknown'
    df['age_unit'].fillna('unknown', inplace=True)
    df['patient_age_numeric'].fillna(0.0, inplace=True)
    df['drug_name_cleaned'].fillna('unknown', inplace=True)
    df['reaction_severity'].fillna('unknown', inplace=True)


    return df