# AI-Assisted Self-Healing Data Platform

A student-scale, AI-assisted, self-healing data platform with multi-database support, built as modular microservices.

## 🏗️ Architecture Overview

This platform consists of several independent microservices that work together to provide:

- **Multi-database support**: PostgreSQL (primary) + MongoDB (secondary for unstructured data)
- **ETL Pipeline**: Python scripts that ingest data and simulate common failures
- **Event-Driven Monitoring**: Kafka publishes pipeline and query events; Celery async workers consume events
- **Self-Healing**: LLM classifies failures and generates fixes
  - Sandbox tables: fixes auto-execute
  - Production tables: fixes require human-in-loop approval
- **Vector Memory**: FAISS stores past queries, fixes, and incidents for retrieval and active learning
- **LLM Query Generation**: Natural language → schema-aware SQL with optimization suggestions
- **Frontend**: React app to run queries, view pipeline health, see fixes, approve sandbox fixes, view query optimizations
- **Orchestration**: Airflow DAG orchestrates ETL pipeline + triggers Celery tasks

## 📁 Project Structure

```
AI-Data-Self-Healing/
├── databases/
│   ├── postgres/
│   │   └── init.sql              # PostgreSQL initialization schema
│   └── mongodb/
│       └── init.js               # MongoDB initialization script
├── services/
│   ├── etl-service/              # ETL Pipeline microservice
│   ├── monitoring-service/       # Event-driven monitoring microservice
│   ├── query-service/            # LLM Query Generation microservice
│   ├── self-healing-service/     # Self-healing microservice
│   ├── vector-memory-service/    # Vector memory (FAISS) microservice
│   └── frontend/                 # React frontend microservice
├── dags/
│   └── etl_pipeline_dag.py       # Airflow DAG for orchestration
├── shared/
│   └── config/                   # Shared configuration files
├── docker-compose.yml            # Docker Compose configuration
└── README.md                     # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- OpenAI API key (optional, for LLM features)
- At least 8GB RAM available for Docker

### Setup

1. **Clone and navigate to the project**:
   ```bash
   cd AI-Data-Self-Healing
   ```

2. **Set environment variables** (optional, for LLM features):
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```

3. **Start all services**:
   ```bash
   docker-compose up -d
   ```

   This will start:
   - PostgreSQL (port 5432)
   - MongoDB (port 27017)
   - Kafka + Zookeeper (port 9092)
   - Redis (port 6379)
   - All microservices (ports 8001-8005)
   - React frontend (port 3000)
   - Airflow webserver (port 8080)
   - Airflow scheduler
   - Celery worker

4. **Wait for services to be healthy** (about 1-2 minutes):
   ```bash
   docker-compose ps
   ```

5. **Access the services**:
   - **Frontend**: http://localhost:3000
   - **Airflow**: http://localhost:8080 (admin/admin)
   - **ETL Service**: http://localhost:8001/docs
   - **Monitoring Service**: http://localhost:8002/docs
   - **Query Service**: http://localhost:8003/docs
   - **Self-Healing Service**: http://localhost:8004/docs
   - **Vector Memory Service**: http://localhost:8005/docs

## 📊 Service Details

### 1. ETL Pipeline Service (Port 8001)

- Ingests data from PostgreSQL and MongoDB
- Simulates common failures (schema drift, missing data)
- Publishes pipeline events to Kafka

**API Endpoints**:
- `POST /pipeline/run` - Trigger ETL pipeline
- `GET /pipeline/runs` - Get all pipeline runs
- `GET /health` - Health check

### 2. Monitoring Service (Port 8002)

- Consumes events from Kafka
- Classifies failures and stores in PostgreSQL
- Publishes fix suggestions to self-healing queue
- Uses Celery for async processing

**API Endpoints**:
- `GET /issues` - Get all data quality issues
- `GET /events` - Get recent pipeline events
- `GET /health` - Health check

### 3. Query Service (Port 8003)

- Receives natural language queries via REST API
- Generates schema-aware SQL queries using LLM
- Provides optimization suggestions
- Publishes query execution events to Kafka

**API Endpoints**:
- `POST /query` - Generate and execute SQL from natural language
- `GET /query/{query_id}` - Get query execution result
- `GET /schema` - Get database schema information
- `GET /health` - Health check

### 4. Self-Healing Service (Port 8004)

- Consumes failure events from Kafka
- Generates fixes using LLM
- Auto-executes fixes in sandbox tables
- Logs suggestions for production tables (requires approval)
- Stores resolutions in vector memory

**API Endpoints**:
- `GET /fixes` - Get all fixes
- `POST /fixes/{fix_id}/approve` - Approve fix for production
- `GET /health` - Health check

### 5. Vector Memory Service (Port 8005)

- Stores embeddings of queries, fixes, and incidents using FAISS
- Provides API for retrieval of similar past cases
- Uses OpenAI embeddings for vectorization

**API Endpoints**:
- `POST /store` - Store item in vector memory
- `POST /search` - Search for similar items
- `GET /items/{item_id}` - Get item by ID
- `GET /stats` - Get vector memory statistics
- `GET /health` - Health check

### 6. Frontend (Port 3000)

React application with the following features:
- **Query Interface**: Natural language query input and results display
- **Pipeline Health**: View pipeline runs and data quality issues
- **Fixes & Resolutions**: View and approve fixes
- **Query Optimizations**: View query optimization suggestions

## 🔄 Workflow

1. **ETL Pipeline**:
   - Airflow DAG triggers ETL pipeline
   - Pipeline ingests data from PostgreSQL and MongoDB
   - Simulates failures (schema drift, missing data)
   - Publishes events to Kafka

2. **Monitoring**:
   - Monitoring service consumes events from Kafka
   - Classifies failures and stores in PostgreSQL
   - Publishes fix suggestions

3. **Self-Healing**:
   - Self-healing service consumes failure events
   - Generates fixes using LLM (with similar past fixes from vector memory)
   - Auto-executes fixes in sandbox
   - Stores fixes in vector memory for future reference
   - Production fixes require approval via frontend

4. **Query Generation**:
   - User submits natural language query via frontend
   - Query service generates SQL using LLM
   - Executes query and provides optimization suggestions
   - Publishes query events to Kafka

## 🧪 Testing

### Test ETL Pipeline

```bash
curl -X POST http://localhost:8001/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "student_data_etl", "simulate_failures": true}'
```

### Test Query Service

```bash
curl -X POST http://localhost:8003/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me all students with grade A", "execute": true}'
```

### Check Pipeline Issues

```bash
curl http://localhost:8002/issues
```

### View Fixes

```bash
curl http://localhost:8004/fixes
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```bash
OPENAI_API_KEY=your-api-key-here
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=studentdb
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_USER=admin
MONGO_PASSWORD=admin123
MONGO_DB=studentdb
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
REDIS_HOST=redis
REDIS_PORT=6379
```

### Database Schema

The platform uses:
- **PostgreSQL**: Production tables (`students`, `courses`, `enrollments`) and sandbox tables (`sandbox.students`, etc.)
- **MongoDB**: Unstructured data (`student_profiles`, `event_logs`)

## 📝 Notes

- **Sandbox vs Production**: Fixes auto-execute in sandbox tables; production fixes require approval
- **LLM Requirements**: OpenAI API key is optional but recommended for full functionality
- **Scalability**: This is a student-scale implementation; not designed for production workloads
- **Local Development**: All services can run independently and be tested separately

## 🐛 Troubleshooting

1. **Services not starting**: Check Docker logs with `docker-compose logs [service-name]`
2. **Database connection errors**: Ensure databases are healthy with `docker-compose ps`
3. **Kafka connection issues**: Wait for Zookeeper and Kafka to fully initialize (30-60 seconds)
4. **LLM errors**: Check if `OPENAI_API_KEY` is set correctly

## 📚 Development

Each microservice is independent and can be developed/tested separately:

```bash
# Test ETL service locally
cd services/etl-service
pip install -r requirements.txt
python main.py

# Test Query service locally
cd services/query-service
pip install -r requirements.txt
python main.py
```

## 🔐 Security Notes

- Default passwords are used for demonstration
- In production, use secure credentials and secrets management
- API endpoints are exposed; add authentication/authorization in production

## 📄 License

This is an educational project for learning microservices architecture and AI-assisted data platforms.

## 🤝 Contributing

This is a student-scale project. Contributions and improvements are welcome!
