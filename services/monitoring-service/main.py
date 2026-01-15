"""
Event-Driven Monitoring Service
A microservice that consumes Kafka events, classifies failures,
stores them in PostgreSQL, and publishes fix suggestions.
"""

import os
import json
import logging
from typing import Dict, List
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
import threading

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Monitoring Service", version="1.0.0")

# Database connection
pg_conn = None
kafka_consumer = None
consumer_thread = None

def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
        database=os.getenv("POSTGRES_DB", "studentdb")
    )

def classify_failure(event_type: str, event_data: Dict) -> Dict:
    """Classify failure type and severity"""
    classification = {
        "category": "unknown",
        "severity": "medium",
        "requires_fix": False,
        "fix_type": None
    }
    
    if "schema" in event_type or "mismatch" in event_type:
        classification.update({
            "category": "schema_error",
            "severity": "high",
            "requires_fix": True,
            "fix_type": "schema_repair"
        })
    elif "missing" in event_type or "null" in event_type.lower():
        classification.update({
            "category": "data_quality",
            "severity": "medium",
            "requires_fix": True,
            "fix_type": "data_repair"
        })
    elif "failed" in event_type:
        classification.update({
            "category": "pipeline_error",
            "severity": "high",
            "requires_fix": True,
            "fix_type": "pipeline_repair"
        })
    elif "quality" in event_type:
        classification.update({
            "category": "data_quality",
            "severity": "medium",
            "requires_fix": True,
            "fix_type": "data_repair"
        })
    
    return classification

def store_failure_event(event: Dict):
    """Store failure event in database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        event_type = event.get("event_type", "")
        event_data = event.get("data", {})
        run_id = event_data.get("run_id")
        
        # Classify the failure
        classification = classify_failure(event_type, event_data)
        
        # Store in pipeline_events table
        cursor.execute("""
            INSERT INTO pipeline_events (run_id, event_type, event_data)
            VALUES (%s, %s, %s)
        """, (run_id, event_type, json.dumps(event_data)))
        
        # If it's a quality issue, also store in data_quality_issues
        if classification["requires_fix"] and event_data.get("table_name"):
            cursor.execute("""
                INSERT INTO data_quality_issues 
                (run_id, table_name, issue_type, issue_description, affected_rows, severity, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                run_id,
                event_data.get("table_name", "unknown"),
                event_data.get("issue_type", classification["category"]),
                event_data.get("issue_description", event_type),
                event_data.get("affected_rows", 0),
                classification["severity"],
                "open"
            ))
            
            # Publish fix suggestion to queue (will be picked up by self-healing service)
            # For now, we'll just log it - in production this would go to a Redis queue
            logger.info(f"Publishing fix suggestion for issue: {event_data.get('table_name')}")
            # In a real system, this would be: publish_to_fix_queue(event_data, classification)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Stored failure event: {event_type} for run {run_id}")
        
    except Exception as e:
        logger.error(f"Failed to store failure event: {e}")

def consume_kafka_events():
    """Consume events from Kafka"""
    global kafka_consumer
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    try:
        kafka_consumer = KafkaConsumer(
            "pipeline-events",
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='monitoring-service-group'
        )
        
        logger.info(f"Started consuming from Kafka at {bootstrap_servers}")
        
        for message in kafka_consumer:
            try:
                event = message.value
                event_type = event.get("event_type", "")
                
                logger.info(f"Received event: {event_type}")
                
                # Process failure events
                if "failed" in event_type or "mismatch" in event_type or "missing" in event_type or "quality" in event_type:
                    store_failure_event(event)
                
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")

def start_kafka_consumer():
    """Start Kafka consumer in background thread"""
    global consumer_thread
    if consumer_thread is None or not consumer_thread.is_alive():
        consumer_thread = threading.Thread(target=consume_kafka_events, daemon=True)
        consumer_thread.start()
        logger.info("Started Kafka consumer thread")

@app.on_event("startup")
async def startup():
    """Start Kafka consumer on startup"""
    start_kafka_consumer()

@app.on_event("shutdown")
async def shutdown():
    """Stop Kafka consumer on shutdown"""
    if kafka_consumer:
        kafka_consumer.close()

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "monitoring-service"}

@app.get("/issues")
async def get_issues():
    """Get all data quality issues"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT id, run_id, table_name, issue_type, issue_description, 
                   affected_rows, severity, status, created_at, resolved_at
            FROM data_quality_issues
            ORDER BY created_at DESC
            LIMIT 100
        """)
        issues = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"issues": issues}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events():
    """Get recent pipeline events"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT id, run_id, event_type, event_data, created_at
            FROM pipeline_events
            ORDER BY created_at DESC
            LIMIT 100
        """)
        events = cursor.fetchall()
        cursor.close()
        conn.close()
        return {"events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

