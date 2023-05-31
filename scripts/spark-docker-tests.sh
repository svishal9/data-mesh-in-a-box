docker build . -t test-python --build-arg ARG_RUN_ACTION=tests -f spark_app/Dockerfile
docker run -it test-python