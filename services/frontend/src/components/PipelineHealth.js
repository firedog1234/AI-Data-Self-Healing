import React, { useState, useEffect } from 'react';
import axios from 'axios';

const ETL_SERVICE_URL = 'http://localhost:8001';
const MONITORING_SERVICE_URL = 'http://localhost:8002';

function PipelineHealth() {
  const [runs, setRuns] = useState([]);
  const [issues, setIssues] = useState([]);
  const [loading, setLoading] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    try {
      const [runsRes, issuesRes] = await Promise.all([
        axios.get(`${ETL_SERVICE_URL}/pipeline/runs`),
        axios.get(`${MONITORING_SERVICE_URL}/issues`)
      ]);
      setRuns(runsRes.data.runs || []);
      setIssues(issuesRes.data.issues || []);
    } catch (err) {
      console.error('Failed to fetch pipeline data:', err);
    } finally {
      setLoading(false);
    }
  };

  const triggerPipeline = async () => {
    setLoading(true);
    try {
      await axios.post(`${ETL_SERVICE_URL}/pipeline/run`, {
        pipeline_name: 'student_data_etl',
        simulate_failures: true
      });
      setTimeout(fetchData, 2000); // Refresh after 2 seconds
    } catch (err) {
      console.error('Failed to trigger pipeline:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const getStatusBadge = (status) => {
    const badges = {
      'completed': 'badge-success',
      'completed_with_errors': 'badge-warning',
      'failed': 'badge-danger',
      'running': 'badge-info'
    };
    return badges[status] || 'badge-info';
  };

  return (
    <div>
      <div className="card">
        <h2>Pipeline Health</h2>
        <button 
          className="btn btn-primary" 
          onClick={triggerPipeline}
          disabled={loading}
        >
          Trigger ETL Pipeline
        </button>
      </div>

      <div className="card">
        <h2>Recent Pipeline Runs</h2>
        {loading && <div className="loading">Loading...</div>}
        {runs.length > 0 ? (
          <table className="table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Pipeline Name</th>
                <th>Status</th>
                <th>Records Processed</th>
                <th>Records Failed</th>
                <th>Started At</th>
                <th>Completed At</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.run_id}>
                  <td>{run.run_id.substring(0, 8)}...</td>
                  <td>{run.pipeline_name}</td>
                  <td>
                    <span className={`badge ${getStatusBadge(run.status)}`}>
                      {run.status}
                    </span>
                  </td>
                  <td>{run.records_processed}</td>
                  <td>{run.records_failed}</td>
                  <td>{new Date(run.started_at).toLocaleString()}</td>
                  <td>{run.completed_at ? new Date(run.completed_at).toLocaleString() : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No pipeline runs found.</p>
        )}
      </div>

      <div className="card">
        <h2>Data Quality Issues</h2>
        {issues.length > 0 ? (
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Table Name</th>
                <th>Issue Type</th>
                <th>Severity</th>
                <th>Status</th>
                <th>Affected Rows</th>
                <th>Description</th>
                <th>Created At</th>
              </tr>
            </thead>
            <tbody>
              {issues.map((issue) => (
                <tr key={issue.id}>
                  <td>{issue.id}</td>
                  <td>{issue.table_name}</td>
                  <td>{issue.issue_type}</td>
                  <td>
                    <span className={`badge ${getStatusBadge(issue.severity)}`}>
                      {issue.severity}
                    </span>
                  </td>
                  <td>
                    <span className={`badge ${getStatusBadge(issue.status)}`}>
                      {issue.status}
                    </span>
                  </td>
                  <td>{issue.affected_rows}</td>
                  <td>{issue.issue_description?.substring(0, 50)}...</td>
                  <td>{new Date(issue.created_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No data quality issues found.</p>
        )}
      </div>
    </div>
  );
}

export default PipelineHealth;

