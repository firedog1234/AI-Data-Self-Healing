"""
Self-Healing Service
A microservice that consumes failure events, generates fixes using LLM,
auto-executes fixes in sandbox tables, and logs suggestions for production tables.
"""

import os
import uuid
import json
import logging
import threading
from typing import Dict, List, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from openai import OpenAI
import httpx

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Self-Healing Service", version="1.0.0")

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
    logger.warning("OPENAI_API_KEY not set. LLM-based fixes will be limited.")

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

# Kafka consumer and producer
kafka_consumer = None
kafka_producer = None
consumer_thread = None

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

def get_vector_memory_client():
    """Get HTTP client for vector memory service"""
    vector_memory_url = os.getenv("VECTOR_MEMORY_URL", "http://localhost:8005")
    return httpx.AsyncClient(base_url=vector_memory_url, timeout=30.0)

def generate_fix_with_llm(issue: Dict, similar_fixes: List[Dict] = None) -> Dict:
    """Generate fix using LLM"""
    if not openai_client:
        return generate_fix_fallback(issue)
    
    issue_description = issue.get("issue_description", "")
    issue_type = issue.get("issue_type", "")
    table_name = issue.get("table_name", "")
    
    # Build context from similar fixes
    similar_fixes_context = ""
    if similar_fixes:
        similar_fixes_context = "\n\nSimilar past fixes:\n"
        for fix in similar_fixes[:3]:
            similar_fixes_context += f"- {fix.get('content', '')}\n"
    
    system_prompt = f"""You are a database self-healing expert. Given a data quality issue, generate a SQL fix.

Rules:
- For sandbox environment, use schema "sandbox" (e.g., sandbox.students)
- For production environment, only provide suggestions, do not auto-execute
- Fixes should be safe and reversible when possible
- Always validate data integrity
- Provide clear explanation of what the fix does
{similar_fixes_context}

Return a JSON object with:
- fix_type: type of fix (e.g., "schema_repair", "data_repair")
- sql_fix: the SQL statement to fix the issue (for sandbox only)
- explanation: human-readable explanation of the fix
- safety_level: "safe", "moderate", or "risky"
"""

    user_prompt = f"""Issue Type: {issue_type}
Table: {table_name}
Description: {issue_description}

Generate a fix for this issue in the sandbox environment."""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            max_tokens=500,
            response_format={"type": "json_object"}
        )
        
        fix_json = json.loads(response.choices[0].message.content)
        return fix_json
    except Exception as e:
        logger.error(f"LLM fix generation failed: {e}")
        return generate_fix_fallback(issue)

def generate_fix_fallback(issue: Dict) -> Dict:
    """Fallback fix generation using simple patterns"""
    issue_type = issue.get("issue_type", "")
    table_name = issue.get("table_name", "")
    
    if "schema" in issue_type.lower() or "mismatch" in issue_type.lower():
        return {
            "fix_type": "schema_repair",
            "sql_fix": f"-- Schema mismatch detected. Manual review required for table: {table_name}",
            "explanation": "Schema mismatch requires manual intervention to align table structure.",
            "safety_level": "moderate"
        }
    elif "missing" in issue_type.lower() or "null" in issue_type.lower():
        # Simple null handling
        return {
            "fix_type": "data_repair",
            "sql_fix": f"UPDATE sandbox.{table_name} SET email = COALESCE(email, 'unknown@example.com') WHERE email IS NULL;",
            "explanation": "Fill NULL values with default placeholder values.",
            "safety_level": "safe"
        }
    else:
        return {
            "fix_type": "general_repair",
            "sql_fix": f"-- General fix required for {table_name}. Manual review recommended.",
            "explanation": "This issue requires manual review.",
            "safety_level": "moderate"
        }

async def search_similar_fixes(issue: Dict) -> List[Dict]:
    """Search for similar past fixes using vector memory"""
    try:
        vector_client = get_vector_memory_client()
        
        search_query = f"{issue.get('issue_type', '')} {issue.get('issue_description', '')}"
        
        response = await vector_client.post("/search", json={
            "query": search_query,
            "item_type": "fix",
            "top_k": 5
        })
        
        if response.status_code == 200:
            results = response.json()
            return results.get("results", [])
        else:
            logger.warning(f"Vector memory search failed: {response.status_code}")
            return []
    except Exception as e:
        logger.warning(f"Failed to search similar fixes: {e}")
        return []

async def store_fix_in_vector_memory(issue: Dict, fix: Dict):
    """Store fix in vector memory for future reference"""
    try:
        vector_client = get_vector_memory_client()
        
        fix_content = f"{issue.get('issue_type', '')}: {fix.get('explanation', '')} SQL: {fix.get('sql_fix', '')}"
        
        await vector_client.post("/store", json={
            "content": fix_content,
            "item_type": "fix",
            "metadata": {
                "issue_id": issue.get("id"),
                "fix_type": fix.get("fix_type"),
                "table_name": issue.get("table_name"),
                "safety_level": fix.get("safety_level")
            }
        })
    except Exception as e:
        logger.warning(f"Failed to store fix in vector memory: {e}")

def execute_fix_in_sandbox(sql_fix: str) -> Dict:
    """Execute fix in sandbox environment"""
    try:
        # Remove comments and clean SQL
        sql_lines = [line for line in sql_fix.split('\n') if not line.strip().startswith('--')]
        clean_sql = ' '.join(sql_lines).strip()
        
        # Skip if only comments
        if not clean_sql or clean_sql.startswith('--'):
            return {"status": "skipped", "message": "Fix contains only comments, not executed"}
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(clean_sql)
            conn.commit()
            
            rows_affected = cursor.rowcount
            logger.info(f"Executed fix in sandbox: {rows_affected} rows affected")
            
            return {
                "status": "success",
                "rows_affected": rows_affected,
                "message": f"Fix executed successfully in sandbox"
            }
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Fix execution failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "message": f"Fix execution failed: {str(e)}"
            }
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Failed to execute fix: {e}")
        return {"status": "error", "error": str(e)}

async def process_failure_event(event: Dict):
    """Process a failure event and generate fix"""
    try:
        event_data = event.get("data", {})
        run_id = event_data.get("run_id")
        
        # Check if this is a fixable issue
        if not event_data.get("table_name"):
            logger.debug("Skipping event without table_name")
            return
        
        # Get issue from database
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT id, run_id, table_name, issue_type, issue_description, 
                   affected_rows, severity, status
            FROM data_quality_issues
            WHERE run_id = %s AND table_name = %s AND issue_type = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (run_id, event_data.get("table_name"), event_data.get("issue_type")))
        issue = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not issue:
            logger.warning(f"Issue not found in database for event")
            return
        
        # Skip if already resolved
        if issue["status"] != "open":
            logger.debug(f"Issue {issue['id']} already resolved")
            return
        
        logger.info(f"Processing issue {issue['id']}: {issue['issue_type']}")
        
        # Search for similar fixes
        similar_fixes = await search_similar_fixes(dict(issue))
        
        # Generate fix
        fix = generate_fix_with_llm(dict(issue), similar_fixes)
        
        # Determine environment
        environment = "sandbox"  # Auto-execute in sandbox
        if environment == "sandbox" and fix.get("sql_fix") and not fix.get("sql_fix", "").startswith("--"):
            # Execute fix in sandbox
            execution_result = execute_fix_in_sandbox(fix["sql_fix"])
            fix["execution_status"] = execution_result["status"]
        else:
            fix["execution_status"] = "pending_approval"
        
        # Store fix in database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO fixes 
            (issue_id, fix_type, original_query, suggested_fix, execution_status, environment, approved)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            issue["id"],
            fix.get("fix_type", "unknown"),
            issue["issue_description"],
            json.dumps(fix),
            fix.get("execution_status", "pending"),
            environment,
            environment == "sandbox"  # Auto-approve sandbox fixes
        ))
        conn.commit()
        cursor.close()
        conn.close()
        
        # Store in vector memory
        await store_fix_in_vector_memory(dict(issue), fix)
        
        # Publish fix generated event
        producer = get_kafka_producer()
        producer.send("fix-suggestions", value={
            "event_id": str(uuid.uuid4()),
            "event_type": "fix.generated",
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "issue_id": issue["id"],
                "fix": fix,
                "environment": environment
            }
        })
        producer.flush()
        
        logger.info(f"Generated fix for issue {issue['id']}")
        
    except Exception as e:
        logger.error(f"Failed to process failure event: {e}")

def consume_failure_events():
    """Consume failure events from Kafka"""
    global kafka_consumer
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    
    try:
        kafka_consumer = KafkaConsumer(
            "pipeline-events",
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='self-healing-service-group'
        )
        
        logger.info(f"Started consuming failure events from Kafka at {bootstrap_servers}")
        
        for message in kafka_consumer:
            try:
                event = message.value
                event_type = event.get("event_type", "")
                
                # Process failure-related events
                if any(keyword in event_type.lower() for keyword in ["failed", "mismatch", "missing", "quality"]):
                    logger.info(f"Processing failure event: {event_type}")
                    try:
                        import asyncio
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(process_failure_event(event))
                        loop.close()
                    except Exception as e:
                        logger.error(f"Error in async event processing: {e}")
                
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")

def start_kafka_consumer():
    """Start Kafka consumer in background thread"""
    global consumer_thread
    if consumer_thread is None or not consumer_thread.is_alive():
        consumer_thread = threading.Thread(target=consume_failure_events, daemon=True)
        consumer_thread.start()
        logger.info("Started Kafka consumer thread")

@app.on_event("startup")
async def startup():
    """Start Kafka consumer on startup"""
    get_kafka_producer()
    start_kafka_consumer()

@app.on_event("shutdown")
async def shutdown():
    """Stop Kafka consumer on shutdown"""
    if kafka_consumer:
        kafka_consumer.close()

# Pydantic models
class ApproveFixRequest(BaseModel):
    fix_id: int
    approved: bool = True
    approved_by: str = "admin"

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "self-healing-service"}

@app.get("/fixes")
async def get_fixes():
    """Get all fixes"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT f.id, f.issue_id, f.fix_type, f.original_query, f.suggested_fix,
                   f.execution_status, f.environment, f.approved, f.approved_by,
                   f.created_at, f.executed_at,
                   dqi.table_name, dqi.issue_type, dqi.severity
            FROM fixes f
            JOIN data_quality_issues dqi ON f.issue_id = dqi.id
            ORDER BY f.created_at DESC
            LIMIT 100
        """)
        fixes = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Parse JSON fields
        for fix in fixes:
            if fix["suggested_fix"]:
                fix["suggested_fix"] = json.loads(fix["suggested_fix"])
        
        return {"fixes": fixes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/fixes/{fix_id}/approve")
async def approve_fix(fix_id: int, request: ApproveFixRequest):
    """Approve or reject a fix for production"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get fix details
        cursor.execute("""
            SELECT f.id, f.issue_id, f.suggested_fix, f.environment, dqi.table_name
            FROM fixes f
            JOIN data_quality_issues dqi ON f.issue_id = dqi.id
            WHERE f.id = %s
        """, (fix_id,))
        fix = cursor.fetchone()
        
        if not fix:
            raise HTTPException(status_code=404, detail="Fix not found")
        
        if fix["environment"] != "sandbox":
            raise HTTPException(status_code=400, detail="Only sandbox fixes can be approved for production")
        
        # Update fix approval status
        cursor.execute("""
            UPDATE fixes 
            SET approved = %s, approved_by = %s
            WHERE id = %s
        """, (request.approved, request.approved_by, fix_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"status": "approved" if request.approved else "rejected", "fix_id": fix_id}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)

