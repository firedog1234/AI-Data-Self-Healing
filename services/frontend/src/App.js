import React, { useState } from 'react';
import './index.css';
import QueryInterface from './components/QueryInterface';
import PipelineHealth from './components/PipelineHealth';
import FixesView from './components/FixesView';
import QueryOptimizations from './components/QueryOptimizations';

const QUERY_SERVICE_URL = process.env.REACT_APP_QUERY_SERVICE_URL || 'http://localhost:8003';
const SELF_HEALING_SERVICE_URL = process.env.REACT_APP_SELF_HEALING_SERVICE_URL || 'http://localhost:8004';

function App() {
  const [activeTab, setActiveTab] = useState('query');

  return (
    <div className="container">
      <div className="header">
        <h1>AI-Assisted Self-Healing Data Platform</h1>
        <nav className="nav">
          <a 
            href="#query" 
            className={activeTab === 'query' ? 'active' : ''}
            onClick={(e) => { e.preventDefault(); setActiveTab('query'); }}
          >
            Query Interface
          </a>
          <a 
            href="#pipeline" 
            className={activeTab === 'pipeline' ? 'active' : ''}
            onClick={(e) => { e.preventDefault(); setActiveTab('pipeline'); }}
          >
            Pipeline Health
          </a>
          <a 
            href="#fixes" 
            className={activeTab === 'fixes' ? 'active' : ''}
            onClick={(e) => { e.preventDefault(); setActiveTab('fixes'); }}
          >
            Fixes & Resolutions
          </a>
          <a 
            href="#optimizations" 
            className={activeTab === 'optimizations' ? 'active' : ''}
            onClick={(e) => { e.preventDefault(); setActiveTab('optimizations'); }}
          >
            Query Optimizations
          </a>
        </nav>
      </div>

      {activeTab === 'query' && (
        <QueryInterface serviceUrl={QUERY_SERVICE_URL} />
      )}
      {activeTab === 'pipeline' && (
        <PipelineHealth />
      )}
      {activeTab === 'fixes' && (
        <FixesView serviceUrl={SELF_HEALING_SERVICE_URL} />
      )}
      {activeTab === 'optimizations' && (
        <QueryOptimizations serviceUrl={QUERY_SERVICE_URL} />
      )}
    </div>
  );
}

export default App;

