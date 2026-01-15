# Quick Start Guide

## Prerequisites

- Docker Desktop installed and running
- At least 8GB RAM available
- OpenAI API key (optional, but recommended for LLM features)

## Step 1: Set Environment Variables

Create a `.env` file in the root directory (or export variables):

```bash
export OPENAI_API_KEY="your-api-key-here"
```

Or create `.env`:
```
OPENAI_API_KEY=your-api-key-here
```

## Step 2: Start All Services

```bash
docker-compose up -d
```

This will start all services. Wait 1-2 minutes for everything to initialize.

## Step 3: Verify Services Are Running

```bash
docker-compose ps
```

All services should show "Up" or "healthy" status.

## Step 4: Access the Frontend

Open your browser to: **http://localhost:3000**

You should see the AI Data Self-Healing Platform interface.

## Step 5: Test the Platform

### Test Query Interface

1. Go to "Query Interface" tab
2. Enter a natural language query like: "Show me all students"
3. Click "Execute Query"
4. View the generated SQL and results

### Test Pipeline

1. Go to "Pipeline Health" tab
2. Click "Trigger ETL Pipeline"
3. Watch the pipeline run and see data quality issues appear
4. Fixes will be automatically generated and executed in sandbox

### View Fixes

1. Go to "Fixes & Resolutions" tab
2. See all generated fixes
3. Approve fixes for production if needed

## Step 6: Monitor with Airflow

1. Open Airflow UI: **http://localhost:8080**
2. Login with: `admin` / `admin`
3. Enable the `etl_pipeline_self_healing` DAG
4. Trigger it manually or wait for scheduled run

## Troubleshooting

### Services won't start
```bash
docker-compose logs [service-name]
```

### Database connection errors
```bash
docker-compose restart postgres mongodb
```

### Kafka connection issues
Wait 30-60 seconds for Kafka to fully initialize, then restart services:
```bash
docker-compose restart kafka zookeeper
```

### Check service health
```bash
curl http://localhost:8001/health  # ETL Service
curl http://localhost:8002/health  # Monitoring Service
curl http://localhost:8003/health  # Query Service
curl http://localhost:8004/health  # Self-Healing Service
curl http://localhost:8005/health  # Vector Memory Service
```

## Stopping Services

```bash
docker-compose down
```

To remove all data (volumes):
```bash
docker-compose down -v
```

## Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Explore the API documentation at service endpoints (e.g., http://localhost:8003/docs)
- Modify the ETL pipeline to simulate different failure scenarios
- Experiment with different natural language queries

