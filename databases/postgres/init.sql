-- Main production tables
CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    grade VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS courses (
    id SERIAL PRIMARY KEY,
    course_code VARCHAR(20) UNIQUE NOT NULL,
    course_name VARCHAR(200) NOT NULL,
    instructor VARCHAR(100),
    credits INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS enrollments (
    id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES students(id),
    course_id INTEGER REFERENCES courses(id),
    enrollment_date DATE,
    grade VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(student_id, course_id)
);

-- Sandbox tables (for auto-fixes)
CREATE SCHEMA IF NOT EXISTS sandbox;

CREATE TABLE IF NOT EXISTS sandbox.students (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    grade VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sandbox.courses (
    id SERIAL PRIMARY KEY,
    course_code VARCHAR(20) UNIQUE NOT NULL,
    course_name VARCHAR(200) NOT NULL,
    instructor VARCHAR(100),
    credits INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sandbox.enrollments (
    id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES sandbox.students(id),
    course_id INTEGER REFERENCES sandbox.courses(id),
    enrollment_date DATE,
    grade VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(student_id, course_id)
);

-- Pipeline monitoring metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) UNIQUE NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_events (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    issue_type VARCHAR(100) NOT NULL,
    issue_description TEXT,
    affected_rows INTEGER,
    severity VARCHAR(20) DEFAULT 'medium',
    status VARCHAR(50) DEFAULT 'open',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
);

-- Query execution tracking
CREATE TABLE IF NOT EXISTS query_executions (
    id SERIAL PRIMARY KEY,
    query_id VARCHAR(100) UNIQUE NOT NULL,
    user_query TEXT NOT NULL,
    generated_sql TEXT,
    execution_time_ms INTEGER,
    rows_returned INTEGER,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fixes and resolutions
CREATE TABLE IF NOT EXISTS fixes (
    id SERIAL PRIMARY KEY,
    issue_id INTEGER REFERENCES data_quality_issues(id),
    fix_type VARCHAR(50) NOT NULL,
    original_query TEXT,
    suggested_fix TEXT,
    execution_status VARCHAR(50),
    environment VARCHAR(20) DEFAULT 'sandbox',
    approved BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed_at TIMESTAMP
);

-- Vector memory items (for embeddings storage)
CREATE TABLE IF NOT EXISTS vector_memory_items (
    id SERIAL PRIMARY KEY,
    item_id VARCHAR(100) UNIQUE NOT NULL,
    content TEXT NOT NULL,
    item_type VARCHAR(50) NOT NULL,
    metadata_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_vector_memory_item_type ON vector_memory_items(item_type);

-- Sample data
INSERT INTO students (name, email, age, grade) VALUES
('Alice Johnson', 'alice.johnson@example.com', 20, 'Sophomore'),
('Bob Smith', 'bob.smith@example.com', 21, 'Junior'),
('Charlie Brown', 'charlie.brown@example.com', 19, 'Freshman'),
('Diana Prince', 'diana.prince@example.com', 22, 'Senior'),
('Eve Wilson', 'eve.wilson@example.com', 20, 'Sophomore')
ON CONFLICT (email) DO NOTHING;

INSERT INTO courses (course_code, course_name, instructor, credits) VALUES
('CS101', 'Introduction to Computer Science', 'Dr. Smith', 3),
('MATH201', 'Calculus II', 'Dr. Jones', 4),
('ENG101', 'English Composition', 'Prof. Williams', 3),
('PHYS301', 'Quantum Mechanics', 'Dr. Brown', 4),
('HIST202', 'World History', 'Prof. Davis', 3)
ON CONFLICT (course_code) DO NOTHING;

INSERT INTO enrollments (student_id, course_id, enrollment_date, grade) VALUES
(1, 1, '2024-01-15', 'A'),
(1, 3, '2024-01-15', 'B'),
(2, 1, '2024-01-15', 'A'),
(2, 2, '2024-01-15', 'A'),
(3, 3, '2024-01-15', 'C'),
(4, 4, '2024-01-15', 'A'),
(5, 5, '2024-01-15', 'B')
ON CONFLICT (student_id, course_id) DO NOTHING;

