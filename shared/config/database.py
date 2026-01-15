"""Database connection configuration"""

import os
from typing import Optional

def get_postgres_connection_string() -> str:
    """Get PostgreSQL connection string from environment variables"""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "admin")
    password = os.getenv("POSTGRES_PASSWORD", "admin123")
    database = os.getenv("POSTGRES_DB", "studentdb")
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

def get_mongo_connection_string() -> str:
    """Get MongoDB connection string from environment variables"""
    host = os.getenv("MONGO_HOST", "localhost")
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_USER", "admin")
    password = os.getenv("MONGO_PASSWORD", "admin123")
    database = os.getenv("MONGO_DB", "studentdb")
    
    return f"mongodb://{user}:{password}@{host}:{port}/{database}?authSource=admin"

