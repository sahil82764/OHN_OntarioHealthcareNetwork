# Zero dependencies — the whole point of the stdlib implementation.
FROM python:3.12-slim
WORKDIR /app
COPY api_server.py .
COPY data ./data
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')"
ENTRYPOINT ["python", "api_server.py", "--data", "./data", "--host", "0.0.0.0", "--port", "8000"]
