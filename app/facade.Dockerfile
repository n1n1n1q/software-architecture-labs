FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV LOGGING_SERVICE_TARGETS=logging-service-1:50051,logging-service-2:50051,logging-service-3:50051
ENV COUNTER_SERVICE_URL=http://counter-service:8001

EXPOSE 8002

CMD ["uvicorn", "facade_service:app", "--host", "0.0.0.0", "--port", "8002"]
