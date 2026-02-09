# AI Agent NAS - Docker Environment

This Docker environment provides an all-in-one container with PostgreSQL, n8n, Python, and uv package manager.

## 🚀 Quick Start

1. **Configure environment variables:**
   ```bash
   cp .env.template .env
   # Edit .env and update passwords and configuration
   ```

2. **Build and start the container:**
   ```bash
   docker compose build
   docker compose up -d
   ```

3. **Access services:**
   - n8n: http://localhost:5678
   - PostgreSQL: localhost:5432
   - n8n-server API: http://localhost:5000

4. **Access container shell:**
   ```bash
   docker exec -it ai-agent-nas bash
   ```

## 📦 What's Included

- **Ubuntu** (latest)
- **PostgreSQL** 14 with two databases:
  - `n8n` - Main n8n database
  - `ai_agent` - AI agent database with vector extensions
- **pgvector** - PostgreSQL extension for vector similarity search
- **Vector Tables**:
  - `documents` - Document storage with embeddings
  - `memories` - Memory storage with embeddings
  - `document_metadata` - Document metadata tracking
  - `document_rows` - Dataset rows storage
- **Vector Search Functions**:
  - `match_documents()` - Similarity search for documents
  - `match_memories()` - Similarity search for memories
- **n8n** - Workflow automation tool
- **Python 3** with pip
- **uv** - Fast Python package installer
- **Node.js 20** - Required for n8n

## 🔧 Configuration

All configuration is managed through the `.env` file:

### Database Configuration
- `POSTGRES_DB` - Primary database name
- `POSTGRES_USER` - Primary database user
- `POSTGRES_PASSWORD` - Primary database password
- `N8N_POSTGRES_DB` - AI agent database name
- `N8N_POSTGRES_USER` - AI agent database user
- `N8N_POSTGRES_PASSWORD` - AI agent database password

### n8n Configuration
- `N8N_PORT` - n8n web interface port (default: 5678)
- `N8N_HOST` - Domain name for n8n
- `N8N_BASIC_AUTH_USER` - Admin username
- `N8N_BASIC_AUTH_PASSWORD` - Admin password

### Other Settings
- `GENERIC_TIMEZONE` - Container timezone
- `VECTOR_SIZE` - pgvector embedding dimension (3072 or 1536)

## 🗄️ Database Initialization

The container automatically initializes PostgreSQL on first run using `init.sql`:
- Creates databases specified in `.env` (`n8n` and `ai_agent`)
- Creates users with encrypted passwords
- Grants necessary privileges and schema permissions (PostgreSQL 15+ compatible)
- **Enables pgvector extension** for vector similarity search
- **Creates vector tables**:
  - `documents` table with configurable embedding dimensions
  - `memories` table for AI agent memory storage
  - `document_metadata` for tracking document information
  - `document_rows` for dataset storage
- **Creates search functions** for vector similarity matching with JSONB filter support

## 📝 Notes

- The `data/` directory is mounted for persistent storage
- PostgreSQL data persists in `/var/lib/postgresql/`
- `.env` is gitignored to protect secrets
- Use `.env.template` for version control

## 🛠️ Starting n8n

To start n8n inside the container:

```bash
docker exec -it ai-agent-nas bash
n8n start
```

Or configure it to start automatically by modifying `init.sh`.

## 📡 n8n-Service API

The container includes a Flask-based SAP sheet parser service that runs on port 5000.

### Endpoints

#### POST /parse-sap-sheet
Parse a Google Drive SAP sheet and write rows to the PostgreSQL database.

**Headers:**
```
Authorization: Bearer YOUR_GOOGLE_OAUTH_TOKEN
Content-Type: application/json
```

**Body:**
```json
{
  "file_id": "YOUR_GOOGLE_DRIVE_FILE_ID",
  "dataset_id": "optional_dataset_identifier"
}
```

**Response:**
```json
{
  "file_id": "1abc123...",
  "name": "SAP_Report.xlsx",
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "rows_written": 42,
  "dataset_id": "sap_dataset_001"
}
```

**Example:**
```bash
curl -X POST http://localhost:5000/parse-sap-sheet \
  -H "Authorization: Bearer ya29.a0AfH6SMB..." \
  -H "Content-Type: application/json" \
  -d '{"file_id":"1abc123def456","dataset_id":"sap_2024_01"}'
```

#### POST /parse-sap-sheet-jsonl
Parse a Google Drive SAP sheet and return as JSONL (no database write).

**Headers:**
```
Authorization: Bearer YOUR_GOOGLE_OAUTH_TOKEN
Content-Type: application/json
```

**Body:**
```json
{
  "file_id": "YOUR_GOOGLE_DRIVE_FILE_ID"
}
```

**Response:**
```json
{
  "file_id": "1abc123...",
  "name": "SAP_Report.xlsx",
  "jsonl_path": "/tmp/sap_1abc123.jsonl",
  "row_count": 42
}
```

### Required Google Drive Scopes
- `https://www.googleapis.com/auth/drive.readonly`

The OAuth user must have read access to the target file in Google Drive.

### Logs
View service logs:
```bash
docker exec -it ai-agent-nas tail -f /var/log/n8n-service.log
```

## 🔒 Security

**IMPORTANT:** Change these default passwords in `.env`:
- `N8N_BASIC_AUTH_PASSWORD`
- `POSTGRES_PASSWORD`
- `N8N_POSTGRES_PASSWORD`

**Google Service Account:**
- The `service_account.json` file is mounted as a read-only volume (not copied into the image)
- Keep this file secure and never commit it to version control
- It is gitignored by default
