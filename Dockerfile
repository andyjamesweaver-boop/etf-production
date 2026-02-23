FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y curl gcc sqlite3 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY *.py ./
RUN mkdir -p data logs

EXPOSE 8080
CMD ["uvicorn", "phase1_production_api:app", "--host", "0.0.0.0", "--port", "8080"]
