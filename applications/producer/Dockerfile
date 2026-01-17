# applications/producer/Dockerfile

FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose metrics port
EXPOSE 8000

# Run the producer
CMD ["python", "-u", "main.py"]
