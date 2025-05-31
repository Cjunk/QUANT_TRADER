FROM python:3.11-slim

WORKDIR /app
COPY bots/macro_metrics/requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]
