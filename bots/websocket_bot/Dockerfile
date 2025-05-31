FROM python:3.11-slim

WORKDIR /app

COPY bots/websocket_bot/requirements.txt .
RUN pip install -r requirements.txt

COPY bots/websocket_bot ./websocket_bot

CMD ["python", "main.py"]


