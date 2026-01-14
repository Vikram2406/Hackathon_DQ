"""
Initialize database - Simple script
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine
from models.database import Base
from dotenv import load_dotenv

load_dotenv()

# Get database URL
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data_quality.db")

print(f"Initializing database: {DATABASE_URL}")

# Create engine
engine = create_engine(DATABASE_URL, echo=True)

# Create all tables
Base.metadata.create_all(bind=engine)

print("\n✅ Database tables created successfully!")
print("\nCreated tables:")
for table in Base.metadata.sorted_tables:
    print(f"  - {table.name}")
