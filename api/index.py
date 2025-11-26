from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import create_engine, text
import pandas as pd
import os

app = FastAPI()

# Load connection string from Environment Variable
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("Database URL not set")
    return create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return RedirectResponse(url="/index.html")

@app.get("/api")
def read_api_root():
    return {"message": "API is running. Go to /api/dashboard-data for data."}

@app.get("/api/dashboard-data")
def get_dashboard_data(
    start_year: int = Query(2021, description="Start year of analysis"), 
    end_year: int = Query(2023, description="End year of analysis")
):
    """
    Fetches data for a date range.
    - Growth is calculated from start_year to end_year.
    - Snapshot charts (Top 10, Scatter) use end_year data.
    - Trend charts use the full range.
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

        # 4. Filter for Range
        df_range = df[(df['year'] >= start_year) & (df['year'] <= end_year)].copy()
        
        # 5. Separation: Regions vs Countries
        region_codes = ['EMU', 'EUU', 'OED', 'CEB', 'EAS', 'LCN', 'MEA', 'NAC', 'SAS', 'SSF', 'WLD']
        df_regions = df_range[df_range['country_iso_code'].isin(region_codes)]
        df_countries = df_range[~df_range['country_iso_code'].isin(region_codes)]

        # --- CALCULATE GROWTH (Start to End) ---
        # Get values at start and end year for each country
        growth_df = df_countries[df_countries['year'].isin([start_year, end_year])].pivot(
            index='country_iso_code', columns='year', values='pct_above_basic'
        )
        
        # Calculate percentage growth: ((End - Start) / Start) * 100
        if start_year in growth_df.columns and end_year in growth_df.columns:
            growth_df['growth'] = ((growth_df[end_year] - growth_df[start_year]) / growth_df[start_year]) * 100
        else:
            growth_df['growth'] = 0
            
        growth_df['growth'] = growth_df['growth'].fillna(0).replace([float('inf'), -float('inf')], 0)

        # --- PREPARE RESPONSE ---
        
        # Snapshot Data (Latest Year in Range)
        latest_year_df = df_countries[df_countries['year'] == end_year].copy()
        
        # A. Top Countries (Snapshot)
        top_advanced = latest_year_df.nlargest(10, 'pct_above_basic')[['country_name', 'pct_above_basic']].to_dict('records')
        
        # B. Digital Divide (Growth over Range)
        # Identify top/bottom performers based on LATEST proficiency
        top_performers = latest_year_df.nlargest(5, 'pct_above_basic')['country_iso_code']
        bottom_performers = latest_year_df.nsmallest(5, 'pct_above_basic')['country_iso_code']
        
        divide_data = {
            "top_tier_avg_growth": growth_df[growth_df.index.isin(top_performers)]['growth'].mean(),
            "bottom_tier_avg_growth": growth_df[growth_df.index.isin(bottom_performers)]['growth'].mean()
        }

        # C. Correlation (Snapshot)
        correlation_data = latest_year_df[['country_name', 'pct_basic', 'pct_above_basic']].to_dict('records')

        # D. Skill Depth Leaders (Snapshot)
        depth_leaders = latest_year_df.nlargest(10, 'skill_depth_ratio')[['country_name', 'skill_depth_ratio']].to_dict('records')

        # E. Regional Trends (Full Range)
        # Group by region and year to get the trend line
        regional_trends = df_regions.groupby(['country_name', 'year'])['pct_above_basic'].mean().reset_index()
        # Pivot for easier frontend consumption: { "Euro Area": [ {year: 2021, val: 40}, ... ] }
        regions_dict = {}
        for region in regional_trends['country_name'].unique():
            regions_dict[region] = regional_trends[regional_trends['country_name'] == region][['year', 'pct_above_basic']].to_dict('records')

        return {
            "start_year": start_year,
            "end_year": end_year,
            "top_advanced": top_advanced,
            "digital_divide": divide_data,
            "correlation": correlation_data,
            "depth_leaders": depth_leaders,
            "regional_trends": regions_dict
        }

    except Exception as e:
        return {"error": str(e)}

# Catch-all for debugging path issues
@app.get("/{path:path}")
def catch_all(path: str):
    return {"message": "Path not found", "path": path}