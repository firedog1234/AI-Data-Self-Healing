"""Kafka topic configuration for the platform"""

# Topic names
PIPELINE_EVENTS_TOPIC = "pipeline-events"
QUERY_EVENTS_TOPIC = "query-events"
FIX_SUGGESTIONS_TOPIC = "fix-suggestions"
MONITORING_ALERTS_TOPIC = "monitoring-alerts"

# Event types
class PipelineEventType:
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_FAILED = "pipeline.failed"
    DATA_INGESTED = "data.ingested"
    SCHEMA_MISMATCH = "schema.mismatch"
    MISSING_DATA = "data.missing"
    DATA_QUALITY_ISSUE = "data.quality.issue"

class QueryEventType:
    QUERY_SUBMITTED = "query.submitted"
    QUERY_EXECUTED = "query.executed"
    QUERY_FAILED = "query.failed"
    QUERY_OPTIMIZED = "query.optimized"

class FixEventType:
    FIX_GENERATED = "fix.generated"
    FIX_APPROVED = "fix.approved"
    FIX_EXECUTED = "fix.executed"
    FIX_REJECTED = "fix.rejected"

