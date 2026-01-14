"""
Database configuration and session management
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

# Database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://dq_user:dq_password@localhost:5432/data_quality")

# Create engine
engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,
    echo=os.getenv("ENVIRONMENT") == "development",
    future=True
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Session:
    """
    Dependency for FastAPI to get database session
    
    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """
    Context manager for database session
    
    Usage:
        with get_db_context() as db:
            db.query(...)
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    from backend.models.database import Base
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully")


def drop_db():
    """Drop all database tables (use with caution!)"""
    from backend.models.database import Base
    Base.metadata.drop_all(bind=engine)
    print("Database tables dropped")
