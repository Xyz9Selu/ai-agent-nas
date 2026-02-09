FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency metadata
COPY pyproject.toml uv.lock ./

# Install dependencies (prod only)
RUN uv sync --frozen --no-dev

# Copy application code
COPY main.py sap_parser.py db.py ./

# Create a non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port (default 5000, can be overridden via PORT env var)
EXPOSE 5000

# Run the application (uv run uses the project venv with installed deps)
CMD ["uv", "run", "python", "main.py"]

