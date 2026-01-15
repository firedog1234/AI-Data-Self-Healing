import React, { useState, useEffect } from 'react';
import axios from 'axios';

function FixesView({ serviceUrl }) {
  const [fixes, setFixes] = useState([]);
  const [loading, setLoading] = useState(false);

  const fetchFixes = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${serviceUrl}/fixes`);
      setFixes(response.data.fixes || []);
    } catch (err) {
      console.error('Failed to fetch fixes:', err);
    } finally {
      setLoading(false);
    }
  };

  const approveFix = async (fixId) => {
    try {
      await axios.post(`${serviceUrl}/fixes/${fixId}/approve`, {
        fix_id: fixId,
        approved: true,
        approved_by: 'admin'
      });
      fetchFixes();
    } catch (err) {
      console.error('Failed to approve fix:', err);
    }
  };

  useEffect(() => {
    fetchFixes();
    const interval = setInterval(fetchFixes, 5000);
    return () => clearInterval(interval);
  }, []);

  const getFixStatusBadge = (status) => {
    const badges = {
      'success': 'badge-success',
      'error': 'badge-danger',
      'pending': 'badge-warning',
      'pending_approval': 'badge-info'
    };
    return badges[status] || 'badge-info';
  };

  return (
    <div className="card">
      <h2>Fixes & Resolutions</h2>
      {loading && <div className="loading">Loading...</div>}
      {fixes.length > 0 ? (
        <table className="table">
          <thead>
            <tr>
              <th>ID</th>
              <th>Issue Type</th>
              <th>Table</th>
              <th>Fix Type</th>
              <th>Environment</th>
              <th>Status</th>
              <th>Approved</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {fixes.map((fix) => (
              <tr key={fix.id}>
                <td>{fix.id}</td>
                <td>{fix.issue_type}</td>
                <td>{fix.table_name}</td>
                <td>{fix.fix_type}</td>
                <td>{fix.environment}</td>
                <td>
                  <span className={`badge ${getFixStatusBadge(fix.execution_status)}`}>
                    {fix.execution_status}
                  </span>
                </td>
                <td>{fix.approved ? 'Yes' : 'No'}</td>
                <td>
                  {fix.suggested_fix && (
                    <details>
                      <summary>View Fix</summary>
                      <div style={{ marginTop: '10px', padding: '10px', background: '#f8f9fa', borderRadius: '4px' }}>
                        <pre style={{ whiteSpace: 'pre-wrap' }}>
                          {typeof fix.suggested_fix === 'string' 
                            ? fix.suggested_fix 
                            : JSON.stringify(fix.suggested_fix, null, 2)}
                        </pre>
                        {fix.environment === 'sandbox' && !fix.approved && (
                          <button
                            className="btn btn-success"
                            onClick={() => approveFix(fix.id)}
                            style={{ marginTop: '10px' }}
                          >
                            Approve for Production
                          </button>
                        )}
                      </div>
                    </details>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p>No fixes found.</p>
      )}
    </div>
  );
}

export default FixesView;

