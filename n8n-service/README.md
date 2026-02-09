# N8N Service - SAP Sheet Parser

A simple Flask web service that downloads a Google Drive spreadsheet (Google Sheet export CSV or XLSX) and parses it into JSON records.

## Features

- Download a Drive file by `file_id` using an OAuth access token (Bearer).
- Parse and clean the sheet data, then return JSON rows.

## Setup

### Prerequisites

- Python 3.11+
- Poetry

### Install dependencies

```bash
poetry install
```

### Google Drive auth

This service uses a **Google Drive OAuth access token** provided per request via `Authorization: Bearer <access_token>`.

- Required scope: `https://www.googleapis.com/auth/drive.readonly`
- The OAuth user must have access to the target file in Google Drive.

### Environment variables (optional)

- `PORT`: Flask server port (default: `5000`)
- `FLASK_DEBUG`: Enable debug mode (default: `False`)

## Run

```bash
poetry run python main.py
```

Service starts at `http://0.0.0.0:5000`.

## Run with Docker

```bash
docker build -t n8n-sap-parser-service .
docker run -d -p 5000:5000 --name n8n-sap-parser-service n8n-sap-parser-service
```

## API

### Parse SAP sheet

```bash
POST /parse-sap-sheet
Authorization: Bearer YOUR_ACCESS_TOKEN
Content-Type: application/json

{
  "file_id": "YOUR_FILE_ID"
}
```

Response:

```json
{
  "file_id": "YOUR_FILE_ID",
  "name": "Your file name",
  "mime_type": "text/csv",
  "rows": [
    { "ColA": "value", "ColB": "value" }
  ]
}
```

Example:

```bash
curl -X POST http://localhost:5000/parse-sap-sheet \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"file_id":"1abc123def456"}'
```

## Error handling

- `400`: Missing `file_id`.
- `401`: Missing/invalid `Authorization` header.
- `5xx`: Google API or parsing failures.

Error response format:

```json
{
  "error": "Error description",
  "message": "Detailed error message"
}
```

