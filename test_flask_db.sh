#!/bin/bash
# Test n8n-service database connection

echo "=== Container Information ==="
docker ps --filter "name=ai-agent-nas" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n=== Environment Variables in Container ===" 
docker exec ai-agent-nas env | grep -E 'RAG_POSTGRES_|POSTGRES_' | sort

echo -e "\n=== Flask Service Logs ==="
docker exec ai-agent-nas cat /var/log/n8n-service.log 2>/dev/null || echo "Log file not yet created"

echo -e "\n=== Testing Database Connection from Python ==="
docker exec ai-agent-nas bash << 'EOF'
cd /app/n8n-service
python3 << 'PYTHON_EOF'
import os
import sys
sys.path.insert(0, "/app/n8n-service")
from db import get_db_connection

print("=== Environment Variables ===")
print(f"RAG_POSTGRES_HOST: {os.getenv('RAG_POSTGRES_HOST')}")
print(f"RAG_POSTGRES_PORT: {os.getenv('RAG_POSTGRES_PORT')}")
print(f"RAG_POSTGRES_DB: {os.getenv('RAG_POSTGRES_DB')}")
print(f"RAG_POSTGRES_USER: {os.getenv('RAG_POSTGRES_USER')}")
print(f"RAG_POSTGRES_PASSWORD: {'***' if os.getenv('RAG_POSTGRES_PASSWORD') else 'NOT SET'}")
print()
print("=== Testing Database Connection ===")
try:
    conn = get_db_connection()
    if conn:
        print("✅ Database connection successful!")
        with conn.cursor() as cur:
            cur.execute('SELECT version();')
            version = cur.fetchone()
            print(f"PostgreSQL version: {version['version']}")
            cur.execute('SELECT current_database();')
            db = cur.fetchone()
            print(f"Connected to database: {db['current_database']}")
            cur.execute('SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename;', ('public',))
            tables = cur.fetchall()
            print(f"Tables in database: {[t['tablename'] for t in tables]}")
        conn.close()
    else:
        print("❌ Database connection returned None")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    import traceback
    traceback.print_exc()
PYTHON_EOF
EOF

echo -e "\n=== Process Status ===" 
docker exec ai-agent-nas ps aux | grep -E 'postgres|n8n|python' | grep -v grep

echo -e "\n=== Test Complete ==="
