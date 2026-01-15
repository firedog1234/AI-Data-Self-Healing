import React, { useState } from 'react';
import axios from 'axios';

function QueryInterface({ serviceUrl }) {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await axios.post(`${serviceUrl}/query`, {
        query: query,
        execute: true
      });

      setResult(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to execute query');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card">
      <h2>Natural Language Query Interface</h2>
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label>Enter your query in natural language:</label>
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g., Show me all students with grade A"
            required
          />
        </div>
        <button type="submit" className="btn btn-primary" disabled={loading}>
          {loading ? 'Processing...' : 'Execute Query'}
        </button>
      </form>

      {error && <div className="error">Error: {error}</div>}

      {result && (
        <div style={{ marginTop: '20px' }}>
          <h3>Generated SQL:</h3>
          <div className="sql-output">{result.generated_sql}</div>

          {result.execution_result && result.execution_result.status === 'success' && (
            <>
              <h3 style={{ marginTop: '20px' }}>Results:</h3>
              <div style={{ marginTop: '10px' }}>
                <p><strong>Rows returned:</strong> {result.execution_result.row_count}</p>
                <p><strong>Execution time:</strong> {result.execution_result.execution_time_ms} ms</p>
              </div>
              {result.execution_result.rows && result.execution_result.rows.length > 0 && (
                <table className="table">
                  <thead>
                    <tr>
                      {Object.keys(result.execution_result.rows[0]).map((key) => (
                        <th key={key}>{key}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.execution_result.rows.map((row, idx) => (
                      <tr key={idx}>
                        {Object.values(row).map((val, i) => (
                          <td key={i}>{String(val)}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}

          {result.execution_result && result.execution_result.status === 'error' && (
            <div className="error" style={{ marginTop: '20px' }}>
              Query execution failed: {result.execution_result.error}
            </div>
          )}

          {result.optimization_suggestions && result.optimization_suggestions.suggestions && 
           result.optimization_suggestions.suggestions.length > 0 && (
            <div style={{ marginTop: '20px' }}>
              <h3>Optimization Suggestions:</h3>
              <ul style={{ marginTop: '10px' }}>
                {result.optimization_suggestions.suggestions.map((suggestion, idx) => (
                  <li key={idx} style={{ marginBottom: '10px' }}>
                    <strong>{suggestion.type}</strong> ({suggestion.severity}): {suggestion.suggestion}
                    <br />
                    <small>Impact: {suggestion.impact}</small>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default QueryInterface;

