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
        
        if df_range.empty:
            return {
                "start_year": start_year,
                "end_year": end_year,
                "top_advanced": [],
                "digital_divide": {"top_tier_avg_growth": 0, "bottom_tier_avg_growth": 0},
                "correlation": [],
                "depth_leaders": [],
                "regional_trends": {}
            }

        # Determine effective snapshot year (latest year with data in range)
        # We prefer the end_year, but if no data exists for it, we take the max available year.
        available_years = df_range['year'].unique()
        snapshot_year = end_year if end_year in available_years else max(available_years)

        # 5. Separation: Regions vs Countries
        region_codes = ['EMU', 'EUU', 'OED', 'CEB', 'EAS', 'LCN', 'MEA', 'NAC', 'SAS', 'SSF', 'WLD']
        df_regions = df_range[df_range['country_iso_code'].isin(region_codes)]
        df_countries = df_range[~df_range['country_iso_code'].isin(region_codes)]

        # --- CALCULATE GROWTH (Start to End) ---
        # Get values at start and end year for each country
        # Note: We use the actual start/end of the selection for growth, even if snapshot is different.
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
        
        # Snapshot Data (Latest Available Year)
        latest_year_df = df_countries[df_countries['year'] == snapshot_year].copy()
        
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
        # Filter out 0s to make the chart cleaner
        correlation_df = latest_year_df[(latest_year_df['pct_basic'] > 0) | (latest_year_df['pct_above_basic'] > 0)]
        correlation_data = correlation_df[['country_name', 'pct_basic', 'pct_above_basic']].to_dict('records')

        # D. Skill Depth Leaders (Snapshot)
        # Filter out 0 ratio
        depth_leaders = latest_year_df[latest_year_df['skill_depth_ratio'] > 0].nlargest(10, 'skill_depth_ratio')[['country_name', 'skill_depth_ratio']].to_dict('records')

        # E. Trends (Dynamic Aggregation)
        # Instead of relying on sparse 'region' rows, we calculate trends from country data.
        
        # 1. Global Average Trend
        global_trend = df_countries.groupby('year')['pct_above_basic'].mean().reset_index()
        
        # 2. Top Performers Trend (Top 10 avg per year)
        # We need to be careful: different countries might be top in different years.
        # But for a trend line, taking the top N of that specific year is a valid "frontier" metric.
        top_trend = df_countries.groupby('year').apply(
            lambda x: x.nlargest(10, 'pct_above_basic')['pct_above_basic'].mean()
        ).reset_index(name='pct_above_basic')

        # 3. Emerging/Low Trend (Bottom 10 avg per year, excluding 0s to avoid noise)
        low_trend = df_countries[df_countries['pct_above_basic'] > 0].groupby('year').apply(
            lambda x: x.nsmallest(10, 'pct_above_basic')['pct_above_basic'].mean()
        ).reset_index(name='pct_above_basic')

        trends_dict = {
            "Global Average": global_trend.to_dict('records'),
            "Top Performers": top_trend.to_dict('records'),
            "Emerging Economies": low_trend.to_dict('records')
        }
        
        # Add original regions if they have enough data points (>= 2) to be useful
        regional_trends_raw = df_regions.groupby(['country_name', 'year'])['pct_above_basic'].mean().reset_index()
        for region in regional_trends_raw['country_name'].unique():
            region_data = regional_trends_raw[regional_trends_raw['country_name'] == region]
            if len(region_data) >= 2: # Only add if it forms a line
                trends_dict[region] = region_data[['year', 'pct_above_basic']].to_dict('records')

        return {
            "start_year": start_year,
            "end_year": end_year,
            "snapshot_year": int(snapshot_year),
            "top_advanced": top_advanced,
            "digital_divide": divide_data,
            "correlation": correlation_data,
            "depth_leaders": depth_leaders,
            "regional_trends": trends_dict
        }

    except Exception as e:
        return {"error": str(e)}

# Catch-all for debugging path issues
@app.get("/{path:path}")
def catch_all(path: str):
    return {"message": "Path not found", "path": path}