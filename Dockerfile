FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipelineforge ./pipelineforge
COPY scripts ./scripts
COPY dags ./dags
RUN mkdir -p /app/data
CMD ["python", "-m", "pipelineforge.run", "demo", "--dir", "/app/data"]
