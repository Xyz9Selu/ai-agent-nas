FROM ubuntu:latest

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# === Generic Settings ===
ENV TZ=Asia/Shanghai

# === n8n Settings ===
ENV N8N_PORT=5678
ENV DOCUMENT_ROWS_TABLE_NAME=document_rows
ENV N8N_HOST=localhost
ENV WEBHOOK_URL=http://localhost:5678
ENV N8N_SECURE_COOKIES=false

# === PostgreSQL Database Settings ===
ENV POSTGRES_DB=n8n
ENV POSTGRES_USER=n8nuser
ENV POSTGRES_PASSWORD=YourSuperSecureDbPassword
ENV RAG_POSTGRES_DB=ai_agent
ENV RAG_POSTGRES_USER=n8nuser
ENV RAG_POSTGRES_PASSWORD=change-this-password
ENV VECTOR_SIZE=3072

# === Email Settings ===
ENV N8N_EMAIL_MODE=smtp
ENV N8N_SMTP_HOST=smtp.gmail.com
ENV N8N_SMTP_PORT=465
ENV N8N_SMTP_USER=your-smtp-user
ENV N8N_SMTP_PASS=your-smtp-password
ENV N8N_SMTP_SENDER=your-sender-email
ENV N8N_SMTP_SSL=true

# Update and install basic dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    lsb-release \
    ca-certificates \
    software-properties-common \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL
RUN apt-get update && apt-get install -y \
    postgresql \
    postgresql-contrib \
    postgresql-server-dev-all \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install pgvector extension
RUN cd /tmp && \
    git clone --branch v0.5.1 https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/pgvector

# Install Python 3 and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create symlinks for python and pip
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Install uv (Python package installer) via pip
RUN pip install --break-system-packages uv

# Install Node.js (required for n8n)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Install n8n globally
RUN npm install -g n8n

# Create working directory
WORKDIR /app

# Copy n8n-service Python application
COPY n8n-service/pyproject.toml n8n-service/uv.lock /app/n8n-service/
WORKDIR /app/n8n-service

# Install Python dependencies using uv (production only)
RUN uv sync --frozen --no-dev

# Copy n8n-service application code
COPY n8n-service/main.py n8n-service/sap_parser.py n8n-service/db.py /app/n8n-service/

# Return to app directory
WORKDIR /app

# Copy initialization script
COPY init.sh /app/init.sh
COPY init.sql /app/init.sql
RUN chmod +x /app/init.sh

# Expose ports
# PostgreSQL default port
EXPOSE 5432
# n8n default port
EXPOSE 5678

# Set default command to run init script then sleep
CMD ["/app/init.sh", "sleep", "infinity"]
