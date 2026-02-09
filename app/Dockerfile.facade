FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV LOGGING_SERVICE_URL=http://logging-service:8000
ENV COUNTER_SERVICE_URL=http://counter-service:8001

EXPOSE 8002

CMD ["uvicorn", "facade_service:app", "--host", "0.0.0.0", "--port", "8002"]
