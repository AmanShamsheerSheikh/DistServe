CREATE TABLE IF NOT EXISTS jobs (
    id          SERIAL PRIMARY KEY,
    job_name    VARCHAR(255) NOT NULL,
    status      VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    worker_id   VARCHAR(255),
    result      JSONB,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_progress (
    id            SERIAL PRIMARY KEY,
    job_id        INTEGER REFERENCES jobs(id) ON DELETE CASCADE,
    epoch         INTEGER,
    total_epochs  INTEGER,
    step          INTEGER,
    loss          FLOAT,
    gpu_memory_gb FLOAT,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_progress_job_id 
ON job_progress(job_id, created_at DESC);