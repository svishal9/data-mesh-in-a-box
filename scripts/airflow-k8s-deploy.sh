eval $(minikube -p minikube docker-env)
docker build . -t dmiab/airflow-deploy:latest -f airflow/Dockerfile
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  --values values.yaml
