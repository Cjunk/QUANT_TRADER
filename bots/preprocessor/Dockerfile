FROM python:3.11-slim

WORKDIR /app

COPY bots/preprocessor/requirements.txt .
RUN pip install -r requirements.txt

COPY bots/preprocessor ./preprocessor

CMD ["python", "main.py"]




