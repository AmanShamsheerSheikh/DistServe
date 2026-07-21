CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_name VARCHAR(255) UNIQUE NOT NULL,
    api_key UUID DEFAULT gen_random_uuid() UNIQUE
);

CREATE TABLE IF NOT EXISTS jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID REFERENCES users(user_id),
    document_id UUID UNIQUE,
    file_name   TEXT,
    num_chunks  INT,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    result      TEXT,
    error       TEXT,
    created_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chunks (
    id          UUID PRIMARY KEY,
    document_id UUID REFERENCES jobs(document_id),
    chunk_index INT,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    address     JSONB,
    source_text TEXT,
    result      TEXT,
    error       TEXT,
    created_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)