FROM python:3.11-slim

WORKDIR /app

COPY bots/db/requirements.txt .
RUN pip install -r requirements.txt

COPY bots/db ./db

CMD ["python", "main.py"]


