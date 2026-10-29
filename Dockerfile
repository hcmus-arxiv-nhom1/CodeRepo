FROM python:3.12-slim

WORKDIR /app
COPY src/requirements.txt /app/src/requirements.txt
RUN pip install --no-cache-dir -r /app/src/requirements.txt
COPY src /app/src

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "src.run_pipeline", "--help"]
