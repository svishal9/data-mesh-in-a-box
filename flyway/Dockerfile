FROM flyway/flyway

ENV JDBC_VERSION 41-1.2.10.1009

USER root
RUN apt-get update \
  && apt-get install -y python python3-pip postgresql\
  && apt-get clean
RUN pip install awscli
RUN mkdir -p /flyway/rsdriver \
  && curl -L https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC${JDBC_VERSION}.jar -o /flyway/rsdriver/RedshiftJDBC${JDBC_VERSION}.jar
RUN chmod 0755 /flyway/flyway
USER flyway
