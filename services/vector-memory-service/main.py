"""
Vector Memory Service
A microservice that stores embeddings of queries, fixes, and incidents
for retrieval and active learning using FAISS.
"""

import os
import uuid
import json
import logging
import pickle
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path

import numpy as np
import faiss
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
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

app = FastAPI(title="Vector Memory Service", version="1.0.0")

# Initialize OpenAI client
openai_client = None
if os.getenv("OPENAI_API_KEY"):
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
else:
    logger.warning("OPENAI_API_KEY not set. Embedding generation will fail.")

# FAISS index
INDEX_PATH = Path("/app/indexes/vector_index.faiss")
METADATA_PATH = Path("/app/indexes/metadata.pkl")
INDEX_DIR = INDEX_PATH.parent
INDEX_DIR.mkdir(parents=True, exist_ok=True)

vector_index = None
metadata_store = {}  # Maps FAISS index ID to metadata

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

def get_embedding(text: str) -> List[float]:
    """Get embedding for text using OpenAI"""
    if not openai_client:
        raise ValueError("OpenAI API key not configured")
    
    try:
        response = openai_client.embeddings.create(
            model="text-embedding-ada-002",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"Failed to get embedding: {e}")
        raise

def initialize_faiss_index(dimension: int = 1536):
    """Initialize or load FAISS index"""
    global vector_index
    
    if INDEX_PATH.exists() and METADATA_PATH.exists():
        # Load existing index
        logger.info("Loading existing FAISS index")
        vector_index = faiss.read_index(str(INDEX_PATH))
        with open(METADATA_PATH, 'rb') as f:
            metadata_store.update(pickle.load(f))
        logger.info(f"Loaded index with {vector_index.ntotal} vectors")
    else:
        # Create new index
        logger.info("Creating new FAISS index")
        vector_index = faiss.IndexFlatL2(dimension)  # L2 distance for similarity search
        logger.info("Created new FAISS index")
    
    return vector_index

def save_faiss_index():
    """Save FAISS index and metadata to disk"""
    try:
        faiss.write_index(vector_index, str(INDEX_PATH))
        with open(METADATA_PATH, 'wb') as f:
            pickle.dump(metadata_store, f)
        logger.info("Saved FAISS index and metadata")
    except Exception as e:
        logger.error(f"Failed to save FAISS index: {e}")

# Pydantic models
class StoreItemRequest(BaseModel):
    content: str
    item_type: str  # "query", "fix", "incident"
    metadata: Optional[Dict] = None

class StoreItemResponse(BaseModel):
    item_id: str
    status: str

class SearchRequest(BaseModel):
    query: str
    item_type: Optional[str] = None
    top_k: int = 5

class SearchResult(BaseModel):
    item_id: str
    content: str
    item_type: str
    metadata: Dict
    similarity_score: float

class SearchResponse(BaseModel):
    results: List[SearchResult]
    total_found: int

@app.on_event("startup")
async def startup():
    """Initialize FAISS index on startup"""
    try:
        initialize_faiss_index()
    except Exception as e:
        logger.error(f"Failed to initialize FAISS index: {e}")

@app.on_event("shutdown")
async def shutdown():
    """Save FAISS index on shutdown"""
    try:
        save_faiss_index()
    except Exception as e:
        logger.error(f"Failed to save FAISS index: {e}")

@app.get("/health")
async def health():
    """Health check endpoint"""
    index_size = vector_index.ntotal if vector_index else 0
    return {
        "status": "healthy",
        "service": "vector-memory-service",
        "index_size": index_size
    }

@app.post("/store", response_model=StoreItemResponse)
async def store_item(request: StoreItemRequest):
    """Store item (query, fix, or incident) in vector memory"""
    try:
        # Generate embedding
        embedding = get_embedding(request.content)
        embedding_array = np.array([embedding], dtype='float32')
        
        # Add to FAISS index
        item_id = str(uuid.uuid4())
        index_id = vector_index.ntotal  # Current size is the new index
        
        vector_index.add(embedding_array)
        
        # Store metadata
        metadata = {
            "item_id": item_id,
            "content": request.content,
            "item_type": request.item_type,
            "metadata": request.metadata or {},
            "created_at": datetime.utcnow().isoformat(),
            "index_id": index_id
        }
        metadata_store[index_id] = metadata
        
        # Store in database for persistence
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO vector_memory_items (item_id, content, item_type, metadata_json)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (item_id) DO UPDATE
            SET content = EXCLUDED.content,
                item_type = EXCLUDED.item_type,
                metadata_json = EXCLUDED.metadata_json
        """, (item_id, request.content, request.item_type, json.dumps(request.metadata or {})))
        conn.commit()
        cursor.close()
        conn.close()
        
        # Save index periodically (every 10 items)
        if vector_index.ntotal % 10 == 0:
            save_faiss_index()
        
        logger.info(f"Stored item {item_id} of type {request.item_type}")
        
        return StoreItemResponse(item_id=item_id, status="stored")
        
    except Exception as e:
        logger.error(f"Failed to store item: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search", response_model=SearchResponse)
async def search_items(request: SearchRequest):
    """Search for similar items in vector memory"""
    try:
        # Generate embedding for query
        query_embedding = get_embedding(request.query)
        query_array = np.array([query_embedding], dtype='float32')
        
        # Search in FAISS index
        k = min(request.top_k, vector_index.ntotal)
        if k == 0:
            return SearchResponse(results=[], total_found=0)
        
        distances, indices = vector_index.search(query_array, k)
        
        # Retrieve results with metadata
        results = []
        for i, (distance, idx) in enumerate(zip(distances[0], indices[0])):
            if idx == -1:  # Invalid index
                continue
            
            if idx in metadata_store:
                metadata = metadata_store[idx]
                # Filter by item_type if specified
                if request.item_type and metadata["item_type"] != request.item_type:
                    continue
                
                # Convert L2 distance to similarity score (0-1, higher is more similar)
                similarity_score = 1 / (1 + distance)
                
                result = SearchResult(
                    item_id=metadata["item_id"],
                    content=metadata["content"],
                    item_type=metadata["item_type"],
                    metadata=metadata.get("metadata", {}),
                    similarity_score=float(similarity_score)
                )
                results.append(result)
        
        logger.info(f"Found {len(results)} results for query")
        
        return SearchResponse(
            results=results,
            total_found=len(results)
        )
        
    except Exception as e:
        logger.error(f"Failed to search items: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/items/{item_id}")
async def get_item(item_id: str):
    """Get item by ID"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT item_id, content, item_type, metadata_json, created_at
            FROM vector_memory_items
            WHERE item_id = %s
        """, (item_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Item not found")
        
        result["metadata"] = json.loads(result["metadata_json"]) if result["metadata_json"] else {}
        del result["metadata_json"]
        
        return {"item": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get vector memory statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT item_type, COUNT(*) as count
            FROM vector_memory_items
            GROUP BY item_type
        """)
        stats = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "total_items": vector_index.ntotal if vector_index else 0,
            "by_type": {row["item_type"]: row["count"] for row in stats}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)

