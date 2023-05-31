ARG PYTHON_VERSION=3.9.4
FROM python:$PYTHON_VERSION
USER root
WORKDIR /opt
RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
    wget https://downloads.lightbend.com/scala/2.13.5/scala-2.13.5.tgz && \
    wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
RUN tar xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
    tar xvf scala-2.13.5.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz
ENV PATH="/opt/jdk-11.0.11+9/bin:/opt/scala-2.13.5/bin:/opt/spark-3.1.1-bin-hadoop3.2/bin:$PATH"
RUN apt-get update && apt-get install -y lsb-release
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update  && \
    apt-get install postgresql postgresql-contrib libpq-dev -y
#RUN #apt-get install postgresql:11 postgresql-client:11 libpq-dev -y

RUN pip3 install pipenv
RUN useradd -u 9999 tiger
WORKDIR /app
RUN chown -R tiger /app
RUN chmod 755 /app
USER tiger
COPY Pipfile Pipfile.lock setup.cfg /app/
COPY go.sh /app/go.sh
COPY scripts /app/scripts
RUN ./go.sh setup
COPY spark_app/com /app/com
ARG ARG_RUN_ACTION
ENV RUN_ACTION=$ARG_RUN_ACTION
#ENV RUN_ACTION=demo
ENTRYPOINT exec ./go.sh $RUN_ACTION
