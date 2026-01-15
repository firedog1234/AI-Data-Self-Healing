"""
LLM Query Generation Service
A microservice that receives natural language queries,
generates schema-aware SQL queries, and provides optimization suggestions.
"""

import os
import uuid
import json
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Query Service", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenAI client
openai_client = None
if os.getenv("OPENAI_API_KEY"):
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
else:
    logger.warning("OPENAI_API_KEY not set. LLM features will be limited.")

# Database connection
def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "admin123"),
        database=os.getenv("POSTGRES_DB", "studentdb")
    )

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

def get_schema_info() -> str:
    """Get database schema information for LLM context"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get table information
        cursor.execute("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name IN ('students', 'courses', 'enrollments')
            ORDER BY table_name, ordinal_position
        """)
        columns = cursor.fetchall()
        
        schema_info = "Database Schema:\n"
        current_table = None
        for col in columns:
            if col["table_name"] != current_table:
                if current_table is not None:
                    schema_info += "\n"
                schema_info += f"Table: {col['table_name']}\n"
                current_table = col["table_name"]
            schema_info += f"  - {col['column_name']}: {col['data_type']} ({'nullable' if col['is_nullable'] == 'YES' else 'not null'})\n"
        
        cursor.close()
        conn.close()
        
        return schema_info
    except Exception as e:
        logger.error(f"Failed to get schema info: {e}")
        return "Schema information unavailable"

def generate_sql_with_llm(natural_language_query: str) -> Dict:
    """Generate SQL query using LLM"""
    if not openai_client:
        # Fallback to simple pattern matching if no OpenAI API key
        return generate_sql_fallback(natural_language_query)
    
    schema_info = get_schema_info()
    
    system_prompt = f"""You are a SQL query generator. Given a natural language query, generate a valid PostgreSQL SQL query.

{schema_info}

Rules:
- Always use proper SQL syntax for PostgreSQL
- Only query tables: students, courses, enrollments
- Return only the SQL query, no explanations
- Use appropriate JOINs when needed
- Handle NULL values appropriately
"""

    user_prompt = f"Convert this natural language query to SQL: {natural_language_query}"

    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        generated_sql = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if generated_sql.startswith("```"):
            lines = generated_sql.split("\n")
            generated_sql = "\n".join(lines[1:-1])
        
        return {
            "sql": generated_sql,
            "confidence": "high"
        }
    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        return generate_sql_fallback(natural_language_query)

def generate_sql_fallback(natural_language_query: str) -> Dict:
    """Fallback SQL generation using simple patterns"""
    query_lower = natural_language_query.lower()
    
    if "all students" in query_lower or "list students" in query_lower:
        return {"sql": "SELECT * FROM students", "confidence": "medium"}
    elif "courses" in query_lower:
        return {"sql": "SELECT * FROM courses", "confidence": "medium"}
    elif "enrollments" in query_lower:
        return {"sql": "SELECT * FROM enrollments", "confidence": "medium"}
    else:
        return {"sql": "SELECT * FROM students LIMIT 10", "confidence": "low"}

def optimize_query(sql: str) -> Dict:
    """Analyze query and provide optimization suggestions"""
    suggestions = []
    
    # Simple optimization checks
    sql_upper = sql.upper()
    
    if "SELECT *" in sql_upper and "LIMIT" not in sql_upper:
        suggestions.append({
            "type": "performance",
            "severity": "medium",
            "suggestion": "Consider using specific column names instead of SELECT * to reduce data transfer",
            "impact": "Reduces network traffic and memory usage"
        })
    
    if "JOIN" not in sql_upper and ("students" in sql or "courses" in sql or "enrollments" in sql):
        suggestions.append({
            "type": "data_quality",
            "severity": "info",
            "suggestion": "Consider using JOINs to combine related data from multiple tables",
            "impact": "Provides more comprehensive results"
        })
    
    if "WHERE" not in sql_upper and "LIMIT" not in sql_upper:
        suggestions.append({
            "type": "performance",
            "severity": "high",
            "suggestion": "Add a WHERE clause or LIMIT to restrict the number of rows returned",
            "impact": "Prevents returning too many rows and improves performance"
        })
    
    # Use LLM for more sophisticated optimization if available
    if openai_client:
        try:
            optimization_prompt = f"""Analyze this SQL query and provide optimization suggestions:

{sql}

Provide 2-3 specific optimization suggestions in JSON format with keys: type, severity, suggestion, impact.
Return only a JSON array, no other text."""

            response = openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a SQL optimization expert. Return only valid JSON arrays."},
                    {"role": "user", "content": optimization_prompt}
                ],
                temperature=0.2,
                max_tokens=300
            )
            
            llm_suggestions = json.loads(response.choices[0].message.content.strip())
            suggestions.extend(llm_suggestions)
        except Exception as e:
            logger.warning(f"LLM optimization failed: {e}")
    
    return {
        "suggestions": suggestions,
        "estimated_cost": "low" if len(suggestions) == 0 else "medium"
    }

def execute_query(sql: str) -> Dict:
    """Execute SQL query and return results"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        start_time = time.time()
        cursor.execute(sql)
        rows = cursor.fetchall()
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Convert rows to list of dicts
        results = [dict(row) for row in rows]
        
        cursor.close()
        conn.close()
        
        return {
            "rows": results,
            "row_count": len(results),
            "execution_time_ms": execution_time_ms,
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return {
            "rows": [],
            "row_count": 0,
            "execution_time_ms": 0,
            "status": "error",
            "error": str(e)
        }

def publish_query_event(query_id: str, event_type: str, event_data: Dict):
    """Publish query event to Kafka"""
    producer = get_kafka_producer()
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "query_id": query_id,
        "timestamp": datetime.utcnow().isoformat(),
        "data": event_data
    }
    try:
        producer.send("query-events", value=event, key=query_id)
        producer.flush()
        logger.info(f"Published query event {event_type} for query {query_id}")
    except Exception as e:
        logger.error(f"Failed to publish query event: {e}")

# Pydantic models
class QueryRequest(BaseModel):
    query: str
    execute: bool = True

class QueryResponse(BaseModel):
    query_id: str
    user_query: str
    generated_sql: str
    execution_result: Optional[Dict] = None
    optimization_suggestions: Optional[Dict] = None
    status: str

@app.on_event("startup")
async def startup():
    """Initialize connections on startup"""
    get_kafka_producer()

@app.on_event("shutdown")
async def shutdown():
    """Close connections on shutdown"""
    if kafka_producer:
        kafka_producer.close()

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "query-service"}

@app.post("/query", response_model=QueryResponse)
async def generate_and_execute_query(request: QueryRequest):
    """Generate SQL from natural language and optionally execute it"""
    query_id = str(uuid.uuid4())
    
    try:
        # Publish query submitted event
        publish_query_event(
            query_id,
            "query.submitted",
            {"user_query": request.query}
        )
        
        # Generate SQL
        sql_result = generate_sql_with_llm(request.query)
        generated_sql = sql_result["sql"]
        
        # Store query execution in database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO query_executions 
            (query_id, user_query, generated_sql, status)
            VALUES (%s, %s, %s, %s)
        """, (query_id, request.query, generated_sql, "pending"))
        conn.commit()
        cursor.close()
        conn.close()
        
        execution_result = None
        optimization_suggestions = None
        
        if request.execute:
            # Get optimization suggestions
            optimization_suggestions = optimize_query(generated_sql)
            
            # Execute query
            execution_result = execute_query(generated_sql)
            
            # Update query execution status
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE query_executions 
                SET status = %s, execution_time_ms = %s, rows_returned = %s, 
                    error_message = %s
                WHERE query_id = %s
            """, (
                execution_result["status"],
                execution_result.get("execution_time_ms", 0),
                execution_result.get("row_count", 0),
                execution_result.get("error"),
                query_id
            ))
            conn.commit()
            cursor.close()
            conn.close()
            
            # Publish query executed event
            event_type = "query.executed" if execution_result["status"] == "success" else "query.failed"
            publish_query_event(query_id, event_type, {
                "generated_sql": generated_sql,
                "execution_time_ms": execution_result.get("execution_time_ms", 0),
                "rows_returned": execution_result.get("row_count", 0),
                "status": execution_result["status"]
            })
            
            # Publish optimization event
            if optimization_suggestions.get("suggestions"):
                publish_query_event(query_id, "query.optimized", optimization_suggestions)
        
        return QueryResponse(
            query_id=query_id,
            user_query=request.query,
            generated_sql=generated_sql,
            execution_result=execution_result,
            optimization_suggestions=optimization_suggestions,
            status=execution_result["status"] if execution_result else "generated"
        )
        
    except Exception as e:
        logger.error(f"Query generation/execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/{query_id}")
async def get_query_result(query_id: str):
    """Get query execution result by ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT query_id, user_query, generated_sql, execution_time_ms, 
                   rows_returned, status, error_message, created_at
            FROM query_executions
            WHERE query_id = %s
        """, (query_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Query not found")
        
        return {"query": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema")
async def get_schema():
    """Get database schema information"""
    return {"schema": get_schema_info()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

