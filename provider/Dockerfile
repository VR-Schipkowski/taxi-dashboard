FROM python:3.12-slim

WORKDIR /app

RUN pip install kafka-python

COPY provider.py .

ENTRYPOINT ["python", "provider.py"]