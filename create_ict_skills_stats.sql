-- Create table for ICT Skills Statistics
-- This table flattens the source data, storing Basic and Above Basic percentages as columns.

CREATE TABLE IF NOT EXISTS ict_skills_stats (
    country_iso_code TEXT NOT NULL,
    country_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    pct_basic FLOAT,
    pct_above_basic FLOAT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (country_iso_code, year)
);

-- Add comments for documentation
COMMENT ON TABLE ict_skills_stats IS 'Stores flattened ICT skills statistics (Basic and Above Basic) per country and year.';
COMMENT ON COLUMN ict_skills_stats.country_iso_code IS 'ISO code of the country (e.g., USA, GBR). Part of the Composite Primary Key.';
COMMENT ON COLUMN ict_skills_stats.year IS 'Year of the observation. Part of the Composite Primary Key.';
COMMENT ON COLUMN ict_skills_stats.pct_basic IS 'Percentage of population with Basic ICT skills.';
COMMENT ON COLUMN ict_skills_stats.pct_above_basic IS 'Percentage of population with Above Basic ICT skills.';
