FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir ".[spark]"
ENV SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT ["python", "-m", "retail_flow.cli"]
