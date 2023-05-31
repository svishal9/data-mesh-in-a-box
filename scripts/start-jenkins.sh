docker build . -t test-jenkins-server -f jenkins/Dockerfile
docker run --name myjenkins -p 8080:8080 -p 50000:50000 --env JAVA_OPTS="-Djava.util.logging.config.file=/var/jenkins_home/log.properties"  test-jenkins-server
