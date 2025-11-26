from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, text
import pandas as pd
import os

app = FastAPI()

@app.get("/")
def read_root():
    return RedirectResponse(url="/index.html")

# --- EXISTING CODE BELOW ---

# Load connection string from Environment Variable
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("Database URL not set")
    return create_engine(DATABASE_URL)

@app.get("/api/dashboard-data")
def get_dashboard_data(year: int = Query(2023, description="Target year for snapshot")):
    """
    Fetches all data, calculates KPIs (YoY, Ratios, Correlations), 
    and returns a consolidated JSON object for the dashboard.
    """
    try:
        engine = get_db_connection()
        
        # 1. Fetch RAW data
        query = "SELECT country_name, country_iso_code, year, pct_basic, pct_above_basic FROM ict_skills_stats"
        df = pd.read_sql(query, engine)
        
        # 2. Data Cleaning
        df['pct_basic'] = pd.to_numeric(df['pct_basic'], errors='coerce').fillna(0)
        df['pct_above_basic'] = pd.to_numeric(df['pct_above_basic'], errors='coerce').fillna(0)

        # 3. KPI: Skill Depth Ratio
        df['skill_depth_ratio'] = df.apply(
            lambda x: round(x['pct_above_basic'] / x['pct_basic'], 2) if x['pct_basic'] > 0 else 0, axis=1
        )

        # 4. KPI: YoY Growth
        df = df.sort_values(by=['country_iso_code', 'year'])
        df['growth_advanced'] = df.groupby('country_iso_code')['pct_above_basic'].pct_change() * 100
        df['growth_advanced'] = df['growth_advanced'].fillna(0).round(2)

        # 5. Separation: Regions vs Countries
        region_codes = ['EMU', 'EUU', 'OED', 'CEB', 'EAS', 'LCN', 'MEA', 'NAC', 'SAS', 'SSF', 'WLD']
        df_regions = df[df['country_iso_code'].isin(region_codes)]
        df_countries = df[~df['country_iso_code'].isin(region_codes)]

        # --- PREPARE RESPONSE ---
        
        current_year_df = df_countries[df_countries['year'] == year].copy()
        
        # A. Top Countries
        top_advanced = current_year_df.nlargest(10, 'pct_above_basic')[['country_name', 'pct_above_basic']].to_dict('records')
        
        # B. Digital Divide
        top_performers = current_year_df.nlargest(5, 'pct_above_basic')['country_iso_code']
        bottom_performers = current_year_df.nsmallest(5, 'pct_above_basic')['country_iso_code']
        
        divide_data = {
            "top_tier_avg_growth": current_year_df[current_year_df['country_iso_code'].isin(top_performers)]['growth_advanced'].mean(),
            "bottom_tier_avg_growth": current_year_df[current_year_df['country_iso_code'].isin(bottom_performers)]['growth_advanced'].mean()
        }

        # C. Correlation
        correlation_data = current_year_df[['country_name', 'pct_basic', 'pct_above_basic']].to_dict('records')

        # D. Skill Depth Leaders
        depth_leaders = current_year_df.nlargest(10, 'skill_depth_ratio')[['country_name', 'skill_depth_ratio']].to_dict('records')

        # E. Regional Index
        regional_data = df_regions[df_regions['year'] == year][['country_name', 'pct_above_basic']].to_dict('records')

        return {
            "year": year,
            "top_advanced": top_advanced,
            "digital_divide": divide_data,
            "correlation": correlation_data,
            "depth_leaders": depth_leaders,
            "regional_index": regional_data
        }

    except Exception as e:
        return {"error": str(e)}