eval $(minikube -p minikube docker-env)
docker build . -t test-python --build-arg ARG_RUN_ACTION=tests -f Dockerfile
kubectl apply -f k8s-manifest/namespace.yaml
kubectl apply -f k8s-manifest/spark-test-deployment.yaml
