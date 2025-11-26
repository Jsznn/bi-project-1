import pandas as pd
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Database connection string. 
# User should replace this with their actual connection string or set the environment variable.
# Format: postgresql://user:pass@host:port/db
DB_CONNECTION_STRING = os.getenv('DATABASE_URL')
CSV_FILE_PATH = 'data/ITU_DH_SKLS_DIG_CONT.csv'
TABLE_NAME = 'ict_skills_stats'

def extract_data(file_path):
    """
    Extracts data from the CSV file.
    """
    print(f"Extracting data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
        print(f"Extracted {len( df)} rows.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None

def transform_data(df):
    """
    Transforms the data:
    1. Filters for 'BASIC' and 'ABOVE_BASIC' skills.
    2. Pivots the table to have separate columns for each skill level.
    3. Renames columns to match the SQL schema.
    4. Handles missing values.
    """
    print("Transforming data...")
    
    # 1. Filter rows
    # Keep only rows where COMP_BREAKDOWN_1 is 'BASIC' or 'ABOVE_BASIC'
    filtered_df = df[df['COMP_BREAKDOWN_1'].isin(['BASIC', 'ABOVE_BASIC'])].copy()
    
    # Ensure OBS_VALUE is numeric, converting non-numeric (like '_Z') to NaN
    filtered_df['OBS_VALUE'] = pd.to_numeric(filtered_df['OBS_VALUE'], errors='coerce')
    
    # 2. Pivot the DataFrame
    # Index: Columns to keep as identifiers (Country, Year)
    # Columns: The column whose values will become new column headers (Skill Level)
    # Values: The value to populate in the new cells (Percentage)
    pivoted_df = filtered_df.pivot_table(
        index=['REF_AREA', 'REF_AREA_LABEL', 'TIME_PERIOD'], 
        columns='COMP_BREAKDOWN_1', 
        values='OBS_VALUE',
        aggfunc='first' # Should be unique per group, but 'first' is safe
    ).reset_index()
    
    # 3. Rename columns
    # Flatten the MultiIndex columns if created by pivot_table (though reset_index helps)
    pivoted_df.columns.name = None 
    
    rename_map = {
        'REF_AREA': 'country_iso_code',
        'REF_AREA_LABEL': 'country_name',
        'TIME_PERIOD': 'year',
        'BASIC': 'pct_basic',
        'ABOVE_BASIC': 'pct_above_basic'
    }
    pivoted_df = pivoted_df.rename(columns=rename_map)
    
    # Ensure all expected columns exist (in case one skill level was missing entirely)
    expected_cols = ['country_iso_code', 'country_name', 'year', 'pct_basic', 'pct_above_basic']
    for col in expected_cols:
        if col not in pivoted_df.columns:
            pivoted_df[col] = None
            
    # 4. Handle missing values
    # Fill NaNs with None (which becomes NULL in SQL) or 0 if preferred.
    # The requirement said "0 or None". For percentages, None is often safer if data is missing, 
    # but 0 is also valid if it implies no skills. 
    # Let's stick to NaN/None for now as it's more accurate for "missing data".
    # If we wanted 0: pivoted_df = pivoted_df.fillna(0)
    
    # Select and reorder columns
    final_df = pivoted_df[expected_cols]
    
    print(f"Transformed data shape: {final_df.shape}")
    return final_df

def load_data(df, table_name, connection_string):
    """
    Loads data into the Supabase database using an upsert (INSERT ... ON CONFLICT).
    """
    print(f"Loading data into {table_name}...")
    
    engine = sqlalchemy.create_engine(connection_string)
    
    # Convert DataFrame to list of dictionaries for bulk insert
    records = df.to_dict(orient='records')
    
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata, autoload_with=engine)
    
    # Create the upsert statement
    stmt = insert(table).values(records)
    
    # Define what to do on conflict (update the values)
    # Exclude primary key columns from the update
    update_dict = {
        c.name: c for c in stmt.excluded 
        if c.name not in ['country_iso_code', 'year']
    }
    
    on_conflict_stmt = stmt.on_conflict_do_update(
        index_elements=['country_iso_code', 'year'], # Primary Key
        set_=update_dict
    )
    
    with engine.connect() as conn:
        result = conn.execute(on_conflict_stmt)
        conn.commit()
        print(f"Upserted {result.rowcount} rows.")

def main():
    # 1. Extract
    df = extract_data(CSV_FILE_PATH)
    if df is None:
        return

    # 2. Transform
    transformed_df = transform_data(df)
    
    # 3. Load
    try:
        load_data(transformed_df, TABLE_NAME, DB_CONNECTION_STRING)
        print("ETL process completed successfully.")
    except Exception as e:
        print(f"Error during loading: {e}")
        print("Please ensure your DB_CONNECTION_STRING is correct and the table exists.")

if __name__ == "__main__":
    main()
