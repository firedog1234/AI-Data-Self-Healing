"""
ETL Pipeline Service
A microservice that ingests data from PostgreSQL and MongoDB,
simulates common failures, and publishes events to Kafka.
"""

import os
import uuid
import json
import logging
import random
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="ETL Pipeline Service", version="1.0.0")

# Database connections
class DatabaseManager:
    def __init__(self):
        self.pg_conn = None
        self.mongo_client = None
        self.mongo_db = None
        
    def connect_postgres(self):
        """Connect to PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "admin"),
                password=os.getenv("POSTGRES_PASSWORD", "admin123"),
                database=os.getenv("POSTGRES_DB", "studentdb")
            )
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def connect_mongo(self):
        """Connect to MongoDB"""
        try:
            connection_string = f"mongodb://{os.getenv('MONGO_USER', 'admin')}:{os.getenv('MONGO_PASSWORD', 'admin123')}@{os.getenv('MONGO_HOST', 'localhost')}:{os.getenv('MONGO_PORT', '27017')}/?authSource=admin"
            self.mongo_client = MongoClient(connection_string)
            self.mongo_db = self.mongo_client[os.getenv("MONGO_DB", "studentdb")]
            logger.info("Connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def close(self):
        """Close all connections"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()

db_manager = DatabaseManager()

# Kafka producer
kafka_producer = None

def get_kafka_producer():
    """Get or create Kafka producer"""
    global kafka_producer
    if kafka_producer is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        kafka_producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
    return kafka_producer

def publish_event(topic: str, event_type: str, event_data: Dict):
    """Publish event to Kafka"""
    producer = get_kafka_producer()
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "data": event_data
    }
    try:
        producer.send(topic, value=event, key=event_data.get("run_id"))
        producer.flush()
        logger.info(f"Published event {event_type} to {topic}")
    except Exception as e:
        logger.error(f"Failed to publish event: {e}")

# Pydantic models
class PipelineRunRequest(BaseModel):
    pipeline_name: str = "student_data_etl"
    simulate_failures: bool = True

class PipelineRunResponse(BaseModel):
    run_id: str
    status: str
    records_processed: int
    records_failed: int

# ETL Functions
def ingest_from_postgres(run_id: str, simulate_failures: bool = True) -> Dict:
    """Ingest data from PostgreSQL"""
    logger.info(f"Starting PostgreSQL ingestion for run {run_id}")
    records_processed = 0
    records_failed = 0
    issues = []
    
    try:
        cursor = db_manager.pg_conn.cursor(cursor_factory=RealDictCursor)
        
        # Simulate schema mismatch failure
        if simulate_failures and random.random() < 0.3:
            logger.warning("Simulating schema mismatch failure")
            try:
                # Try to query a non-existent column
                cursor.execute("SELECT id, name, email, age, non_existent_column FROM students LIMIT 5")
                cursor.fetchall()
            except psycopg2.Error as e:
                records_failed += 5
                issue = {
                    "run_id": run_id,
                    "table_name": "students",
                    "issue_type": "schema_mismatch",
                    "issue_description": f"Column 'non_existent_column' does not exist: {str(e)}",
                    "affected_rows": 5,
                    "severity": "high"
                }
                issues.append(issue)
                publish_event(
                    "pipeline-events",
                    "schema.mismatch",
                    issue
                )
        
        # Simulate missing data failure
        if simulate_failures and random.random() < 0.3:
            logger.warning("Simulating missing data failure")
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM students 
                WHERE email IS NULL OR name IS NULL
            """)
            missing_count = cursor.fetchone()["count"]
            if missing_count > 0:
                issue = {
                    "run_id": run_id,
                    "table_name": "students",
                    "issue_type": "missing_data",
                    "issue_description": f"Found {missing_count} records with NULL email or name",
                    "affected_rows": missing_count,
                    "severity": "medium"
                }
                issues.append(issue)
                records_failed += missing_count
                publish_event(
                    "pipeline-events",
                    "data.missing",
                    issue
                )
        
        # Normal ingestion
        cursor.execute("SELECT id, name, email, age, grade FROM students LIMIT 100")
        students = cursor.fetchall()
        records_processed += len(students)
        
        # Publish ingestion event
        publish_event(
            "pipeline-events",
            "data.ingested",
            {
                "run_id": run_id,
                "source": "postgresql",
                "table_name": "students",
                "records_count": len(students)
            }
        )
        
        logger.info(f"Processed {len(students)} students from PostgreSQL")
        cursor.close()
        
    except Exception as e:
        logger.error(f"PostgreSQL ingestion failed: {e}")
        records_failed += 100  # Estimate
        publish_event(
            "pipeline-events",
            "pipeline.failed",
            {
                "run_id": run_id,
                "source": "postgresql",
                "error": str(e)
            }
        )
        raise
    
    return {
        "records_processed": records_processed,
        "records_failed": records_failed,
        "issues": issues
    }

def ingest_from_mongodb(run_id: str, simulate_failures: bool = True) -> Dict:
    """Ingest data from MongoDB"""
    logger.info(f"Starting MongoDB ingestion for run {run_id}")
    records_processed = 0
    records_failed = 0
    issues = []
    
    try:
        # Simulate missing collection
        if simulate_failures and random.random() < 0.2:
            logger.warning("Simulating missing collection failure")
            try:
                collection = db_manager.mongo_db["non_existent_collection"]
                collection.find_one()
            except Exception as e:
                records_failed += 10
                issue = {
                    "run_id": run_id,
                    "table_name": "non_existent_collection",
                    "issue_type": "collection_not_found",
                    "issue_description": f"Collection not found: {str(e)}",
                    "affected_rows": 10,
                    "severity": "medium"
                }
                issues.append(issue)
                publish_event(
                    "pipeline-events",
                    "schema.mismatch",
                    issue
                )
        
        # Normal ingestion
        profiles = list(db_manager.mongo_db.student_profiles.find().limit(100))
        records_processed += len(profiles)
        
        # Convert ObjectId to string for JSON serialization
        for profile in profiles:
            if "_id" in profile:
                profile["_id"] = str(profile["_id"])
        
        # Publish ingestion event
        publish_event(
            "pipeline-events",
            "data.ingested",
            {
                "run_id": run_id,
                "source": "mongodb",
                "collection_name": "student_profiles",
                "records_count": len(profiles)
            }
        )
        
        logger.info(f"Processed {len(profiles)} profiles from MongoDB")
        
    except Exception as e:
        logger.error(f"MongoDB ingestion failed: {e}")
        records_failed += 100
        publish_event(
            "pipeline-events",
            "pipeline.failed",
            {
                "run_id": run_id,
                "source": "mongodb",
                "error": str(e)
            }
        )
        raise
    
    return {
        "records_processed": records_processed,
        "records_failed": records_failed,
        "issues": issues
    }

def run_pipeline(pipeline_name: str, simulate_failures: bool = True) -> PipelineRunResponse:
    """Run the complete ETL pipeline"""
    run_id = str(uuid.uuid4())
    logger.info(f"Starting pipeline run {run_id}")
    
    try:
        # Publish pipeline started event
        publish_event(
            "pipeline-events",
            "pipeline.started",
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "started_at": datetime.utcnow().isoformat()
            }
        )
        
        # Track pipeline run in database
        cursor = db_manager.pg_conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at)
            VALUES (%s, %s, %s, %s)
        """, (run_id, pipeline_name, "running", datetime.utcnow()))
        db_manager.pg_conn.commit()
        cursor.close()
        
        total_processed = 0
        total_failed = 0
        all_issues = []
        
        # Ingest from PostgreSQL
        pg_result = ingest_from_postgres(run_id, simulate_failures)
        total_processed += pg_result["records_processed"]
        total_failed += pg_result["records_failed"]
        all_issues.extend(pg_result["issues"])
        
        # Ingest from MongoDB
        mongo_result = ingest_from_mongodb(run_id, simulate_failures)
        total_processed += mongo_result["records_processed"]
        total_failed += mongo_result["records_failed"]
        all_issues.extend(mongo_result["issues"])
        
        # Store issues in database
        if all_issues:
            cursor = db_manager.pg_conn.cursor()
            for issue in all_issues:
                cursor.execute("""
                    INSERT INTO data_quality_issues 
                    (run_id, table_name, issue_type, issue_description, affected_rows, severity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    issue["run_id"],
                    issue["table_name"],
                    issue["issue_type"],
                    issue["issue_description"],
                    issue["affected_rows"],
                    issue["severity"],
                    "open"
                ))
            db_manager.pg_conn.commit()
            cursor.close()
        
        # Update pipeline run status
        status = "completed" if total_failed == 0 else "completed_with_errors"
        cursor = db_manager.pg_conn.cursor()
        cursor.execute("""
            UPDATE pipeline_runs 
            SET status = %s, completed_at = %s, records_processed = %s, records_failed = %s
            WHERE run_id = %s
        """, (status, datetime.utcnow(), total_processed, total_failed, run_id))
        db_manager.pg_conn.commit()
        cursor.close()
        
        # Publish pipeline completed event
        publish_event(
            "pipeline-events",
            "pipeline.completed",
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "status": status,
                "records_processed": total_processed,
                "records_failed": total_failed,
                "completed_at": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Pipeline run {run_id} completed: {total_processed} processed, {total_failed} failed")
        
        return PipelineRunResponse(
            run_id=run_id,
            status=status,
            records_processed=total_processed,
            records_failed=total_failed
        )
        
    except Exception as e:
        logger.error(f"Pipeline run {run_id} failed: {e}")
        # Update pipeline run status
        cursor = db_manager.pg_conn.cursor()
        cursor.execute("""
            UPDATE pipeline_runs 
            SET status = %s, completed_at = %s, error_message = %s
            WHERE run_id = %s
        """, ("failed", datetime.utcnow(), str(e), run_id))
        db_manager.pg_conn.commit()
        cursor.close()
        
        # Publish failure event
        publish_event(
            "pipeline-events",
            "pipeline.failed",
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "error": str(e),
                "failed_at": datetime.utcnow().isoformat()
            }
        )
        raise

@app.on_event("startup")
async def startup():
    """Initialize connections on startup"""
    db_manager.connect_postgres()
    db_manager.connect_mongo()
    get_kafka_producer()

@app.on_event("shutdown")
async def shutdown():
    """Close connections on shutdown"""
    db_manager.close()
    if kafka_producer:
        kafka_producer.close()

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "etl-service"}

@app.post("/pipeline/run", response_model=PipelineRunResponse)
async def run_pipeline_endpoint(request: PipelineRunRequest):
    """Trigger ETL pipeline run"""
    try:
        result = run_pipeline(request.pipeline_name, request.simulate_failures)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pipeline/runs")
async def get_pipeline_runs():
    """Get all pipeline runs"""
    try:
        cursor = db_manager.pg_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT run_id, pipeline_name, status, started_at, completed_at, 
                   records_processed, records_failed, error_message
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 100
        """)
        runs = cursor.fetchall()
        cursor.close()
        return {"runs": runs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

