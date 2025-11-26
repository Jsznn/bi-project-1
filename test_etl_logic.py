import pandas as pd
from etl import transform_data, extract_data

def test_transformation():
    print("Running transformation test...")
    
    # Use the actual file for testing logic
    df = extract_data('data/ITU_DH_SKLS_DIG_CONT.csv')
    
    if df is not None:
        transformed_df = transform_data(df)
        
        print("\n--- Transformed Data Preview (First 5 rows) ---")
        print(transformed_df.head().to_string())
        
        print("\n--- Transformed Data Info ---")
        print(transformed_df.info())
        
        # Basic assertions
        assert 'pct_basic' in transformed_df.columns
        assert 'pct_above_basic' in transformed_df.columns
        assert 'country_iso_code' in transformed_df.columns
        assert 'year' in transformed_df.columns
        
        # Check for a specific known case if possible (e.g., Austria 2023)
        # Row 4 in original file: AUT, 2023, BASIC, 22.9956
        # Row 87 in original file: AUT, 2023, ABOVE_BASIC, 53.2072
        
        aut_2023 = transformed_df[
            (transformed_df['country_iso_code'] == 'AUT') & 
            (transformed_df['year'] == 2023)
        ]
        
        if not aut_2023.empty:
            print("\n--- Verification for AUT 2023 ---")
            print(aut_2023.to_string())
            
            basic_val = aut_2023.iloc[0]['pct_basic']
            above_basic_val = aut_2023.iloc[0]['pct_above_basic']
            
            print(f"AUT 2023 Basic: {basic_val} (Expected ~22.9956)")
            print(f"AUT 2023 Above Basic: {above_basic_val} (Expected ~53.2072)")
        else:
            print("Warning: AUT 2023 data not found in transformed dataframe.")

if __name__ == "__main__":
    test_transformation()
