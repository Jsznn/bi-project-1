import sqlalchemy
from sqlalchemy import text
import os

# Read SQL
with open('create_ict_skills_stats.sql', 'r') as f:
    sql = f.read()

# Connect
db_string = os.getenv('DB_CONNECTION_STRING', 'postgresql://postgres:geminganalog@localhost:5432/postgres')
engine = sqlalchemy.create_engine(db_string)

# Execute
try:
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
        print("Table 'ict_skills_stats' created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")
