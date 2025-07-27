FROM python:3.11-slim

WORKDIR /app

# Add net-tools for netstat
RUN apt-get update && apt-get install -y net-tools curl && apt-get clean

COPY bots/db/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bots/db ./db
COPY bots/utils ./utils
COPY bots/config ./config

ENV PYTHONPATH=/app
EXPOSE 8001
CMD ["python", "main.py"]



