#!/bin/bash
set -e

echo "Starting initialization script..."

# Set defaults
RAG_POSTGRES_DB="${RAG_POSTGRES_DB:-ai_agent}"
RAG_POSTGRES_USER="${RAG_POSTGRES_USER:-n8nuser}"
RAG_POSTGRES_PASSWORD="${RAG_POSTGRES_PASSWORD:?RAG_POSTGRES_PASSWORD must be set}"
VECTOR_SIZE="${VECTOR_SIZE:-3072}"
POSTGRES_USER="${POSTGRES_USER:-n8nuser}"
POSTGRES_DB="${POSTGRES_DB:-n8n}"

# Start PostgreSQL service
echo "Starting PostgreSQL service..."

# Ensure PostgreSQL data directory has proper ownership
if [ -d "/var/lib/postgresql" ]; then
    echo "Setting PostgreSQL directory ownership..."
    chown -R postgres:postgres /var/lib/postgresql
fi

# Check if PostgreSQL cluster is initialized
if [ ! -f "/var/lib/postgresql/16/main/PG_VERSION" ]; then
    echo "PostgreSQL cluster not found, initializing..."
    
    # Create directory structure
    mkdir -p /var/lib/postgresql/16/main
    chown -R postgres:postgres /var/lib/postgresql
    
    # Initialize PostgreSQL cluster as postgres user
    sudo -u postgres /usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/16/main
    
    # Set proper permissions
    chmod -R 700 /var/lib/postgresql/16/main
    
    echo "PostgreSQL cluster initialized successfully"
else
    echo "PostgreSQL cluster already initialized"
fi

service postgresql start

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
sleep 5

# Check if init.sql exists
INIT_SQL="/app/init.sql"
if [[ -f "$INIT_SQL" ]]; then
    echo "Running database initialization from init.sql..."
    
    # Substitute placeholders in init.sql (use | delimiter to avoid issues with / in password)
    sed -e "s/__RAG_POSTGRES_DB__/${RAG_POSTGRES_DB}/g" \
        -e "s/__RAG_POSTGRES_USER__/${RAG_POSTGRES_USER}/g" \
        -e "s/__RAG_POSTGRES_PASSWORD__/${RAG_POSTGRES_PASSWORD}/g" \
        -e "s/__POSTGRES_DB__/${POSTGRES_DB}/g" \
        -e "s/__POSTGRES_USER__/${POSTGRES_USER}/g" \
        -e "s/__POSTGRES_PASSWORD__/${POSTGRES_PASSWORD}/g" \
        -e "s/__VECTOR_SIZE__/${VECTOR_SIZE}/g" \
        "$INIT_SQL" | sudo -u postgres psql -v ON_ERROR_STOP=1 -d postgres
    
    echo "Database initialization from init.sql completed successfully."
else
    echo "Warning: init.sql not found at $INIT_SQL"
    echo "Creating basic databases without vector extensions..."
    
    # Fallback: Create basic database (n8n) if init.sql is missing
    sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB}'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE DATABASE ${POSTGRES_DB};"
    
    # Create user if doesn't exist
    sudo -u postgres psql -tc "SELECT 1 FROM pg_user WHERE usename = '${POSTGRES_USER}'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';"
    
    # Grant privileges
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_USER};"
fi

echo "PostgreSQL initialization complete!"
echo "Databases configured: ${POSTGRES_DB}, ${RAG_POSTGRES_DB}"
echo "Users configured: ${POSTGRES_USER}, ${RAG_POSTGRES_USER}"

# Start n8n-service Flask application
echo "Starting n8n-service Flask application on port ${PORT:-5000}..."
cd /app/n8n-service
nohup uv run python main.py > /var/log/n8n-service.log 2>&1 &
FLASK_PID=$!
echo "Flask service started with PID: $FLASK_PID"

# Wait a moment and check if the service is running
sleep 2
if ps -p $FLASK_PID > /dev/null; then
    echo "Flask service is running successfully"
else
    echo "Warning: Flask service may have failed to start. Check /var/log/n8n-service.log"
fi

# Set n8n internal variables (derived from user-facing env vars)
export GENERIC_TIMEZONE="${TZ:-Asia/Shanghai}"
export DB_TYPE=postgresdb
export DB_POSTGRESDB_HOST=localhost
export DB_POSTGRESDB_PORT=5432
export DB_POSTGRESDB_DATABASE="${POSTGRES_DB}"
export DB_POSTGRESDB_USER="${POSTGRES_USER}"
export DB_POSTGRESDB_PASSWORD="${POSTGRES_PASSWORD}"

# Start n8n
echo "Starting n8n on port ${N8N_PORT:-5678}..."
cd /app
nohup n8n start > /var/log/n8n.log 2>&1 &
N8N_PID=$!
echo "n8n started with PID: $N8N_PID"

# Wait a moment and check if n8n is running
sleep 3
if ps -p $N8N_PID > /dev/null; then
    echo "n8n is running successfully"
else
    echo "Warning: n8n may have failed to start. Check /var/log/n8n.log"
fi

# Keep container running
echo "Container is ready. All services are running."
echo "  - PostgreSQL: port 5432"
echo "  - n8n-service: port ${PORT:-5000}"
echo "  - n8n: port ${N8N_PORT:-5678}"
exec "$@"
