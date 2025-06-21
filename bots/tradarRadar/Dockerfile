FROM python:3.11-slim

WORKDIR /app

COPY bots/tradarRadar/requirements.txt .
RUN pip install -r requirements.txt

COPY bots/tradarRadar ./tradarRadar

CMD ["python", "radar_runner.py"]




