-- Database init: create databases, users, grants, pgvector extension, and tables.
-- Placeholders: __N8N_POSTGRES_DB__, __N8N_POSTGRES_USER__, __N8N_POSTGRES_PASSWORD__, __VECTOR_SIZE__
-- Also uses __POSTGRES_DB__, __POSTGRES_USER__, __POSTGRES_PASSWORD__ from init.sh
-- Substituted by init.sh before execution.

-- 1. Create n8n database for n8n workflows (uses POSTGRES_DB variable)
SELECT 'CREATE DATABASE __POSTGRES_DB__' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '__POSTGRES_DB__')\gexec

-- 2. Create ai_agent database if not exists
SELECT 'CREATE DATABASE __N8N_POSTGRES_DB__' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '__N8N_POSTGRES_DB__')\gexec

-- 3. Create user if not exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '__N8N_POSTGRES_USER__') THEN
    EXECUTE format('CREATE USER %I WITH ENCRYPTED PASSWORD %L', '__N8N_POSTGRES_USER__', '__N8N_POSTGRES_PASSWORD__');
  END IF;
END $$;

-- 4. Grant all privileges on both databases
GRANT ALL PRIVILEGES ON DATABASE __N8N_POSTGRES_DB__ TO __N8N_POSTGRES_USER__;
GRANT ALL PRIVILEGES ON DATABASE __POSTGRES_DB__ TO __N8N_POSTGRES_USER__;
GRANT ALL PRIVILEGES ON DATABASE __N8N_POSTGRES_DB__ TO __N8N_POSTGRES_USER__;

-- 5. Connect to n8n database first to grant schema permissions
\connect __POSTGRES_DB__

-- 6. Grant schema permissions on n8n database (required for PostgreSQL 15+)
GRANT ALL ON SCHEMA public TO __N8N_POSTGRES_USER__;
GRANT USAGE ON SCHEMA public TO __N8N_POSTGRES_USER__;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO __N8N_POSTGRES_USER__;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO __N8N_POSTGRES_USER__;

-- 7. Connect to ai_agent database for schema and extension setup
\connect __N8N_POSTGRES_DB__

-- 8. Grant schema permissions (required for PostgreSQL 15+)
GRANT ALL ON SCHEMA public TO __N8N_POSTGRES_USER__;
GRANT USAGE ON SCHEMA public TO __N8N_POSTGRES_USER__;

-- 6. Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- 7. Create documents table with vector embeddings
CREATE TABLE IF NOT EXISTS documents (
  id bigserial primary key,
  content text,
  metadata jsonb,
  embedding vector(__VECTOR_SIZE__)
);

-- 8. Create match_documents function for vector similarity search
CREATE OR REPLACE FUNCTION match_documents (
  query_embedding vector(__VECTOR_SIZE__),
  match_count int default null,
  filter jsonb DEFAULT '{}'
) RETURNS TABLE (
  id bigint,
  content text,
  metadata jsonb,
  similarity float
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT
    d.id,
    d.content,
    d.metadata,
    1 - (d.embedding <=> query_embedding) AS similarity
  FROM documents d
  WHERE d.metadata @> filter
  ORDER BY d.embedding <=> query_embedding
  LIMIT match_count;
END;
$$;

-- 9. Create memories table with vector embeddings
CREATE TABLE IF NOT EXISTS memories (
  id bigserial primary key,
  content text,
  metadata jsonb,
  embedding vector(__VECTOR_SIZE__)
);

-- 10. Create match_memories function for vector similarity search
CREATE OR REPLACE FUNCTION match_memories (
  query_embedding vector(__VECTOR_SIZE__),
  match_count int default null,
  filter jsonb DEFAULT '{}'
) RETURNS TABLE (
  id bigint,
  content text,
  metadata jsonb,
  similarity float
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  SELECT
    m.id,
    m.content,
    m.metadata,
    1 - (m.embedding <=> query_embedding) AS similarity
  FROM memories m
  WHERE m.metadata @> filter
  ORDER BY m.embedding <=> query_embedding
  LIMIT match_count;
END;
$$;

-- 11. Create document_metadata table
CREATE TABLE IF NOT EXISTS document_metadata (
  id TEXT PRIMARY KEY,
  title TEXT,
  url TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  schema TEXT
);

-- 12. Create document_rows table
CREATE TABLE IF NOT EXISTS document_rows (
  id SERIAL PRIMARY KEY,
  dataset_id TEXT,
  row_data JSONB
);

-- 13. Grant privileges on all tables and sequences
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO __N8N_POSTGRES_USER__;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO __N8N_POSTGRES_USER__;
