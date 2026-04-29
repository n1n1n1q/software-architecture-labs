# Homework 5

Ця реалізація переводить систему на Kubernetes і прибирає статичні IP/порти між мікросервісами.


## Installation

Підняти minicube:

```bash
minikube start --cpus=4 --memory=8192 --driver=docker
minikube status
kubectl cluster-info
```  

Білд докер імеджів:
```bash
docker build -t software-architecture-labs/facade-service:lab5 -f app/facade.Dockerfile app
docker build -t software-architecture-labs/logging-service:lab5 -f app/logging.Dockerfile app
docker build -t software-architecture-labs/counter-service:lab5 -f app/counter.Dockerfile app
```

Завантаження їх у Minikube:
```bash
minikube image load software-architecture-labs/facade-service:lab5
minikube image load software-architecture-labs/counter-service:lab5
minikube image load software-architecture-labs/logging-service:lab5
```


```bash

kubectl apply -k k8s

kubectl get ns

kubectl -n micro-lab5 get all --sort-by=.metadata.creationTimestamp
```


```bash
kubectl -n micro-lab5 get pods -w

kubectl -n micro-lab5 describe pod <pod-name>
kubectl -n micro-lab5 logs <pod-name>
```

Коли всі pods будуть Ready, можна йти далі.


```bash
kubectl -n micro-lab5 port-forward svc/facade-service 8002:8002
```

## Testing

### Basic HTTP

### Fail protection
```bash
kubectl get deploy -n micro-lab5
kubectl scale deploy/facade-service --replicas=2 -n micro-lab5
kubectl scale deploy/logging-service --replicas=2 -n micro-lab5
kubectl scale deploy/counter-service --replicas=2 -n micro-lab5
```


## Performance 
```bash
curl -X POST http://localhost:8002/metrics/reset

python perf_client.py \
  --base-url http://localhost:8002 \
  --clients 10 \
  --requests-per-client 1000 \
  --verify

```

```bash
curl -X POST http://localhost:8002/metrics/reset

python perf_client.py \
  --base-url http://localhost:8002 \
  --clients 10 \
  --requests-per-client 1000 \
  --same-user \
  --verify
```

