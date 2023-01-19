#instructions here
#sudo chmod 0777 /var/run/docker.sock
#docker build --no-cache -t cdspark .
#docker run --name=testApplication -it cdspark:latest
#docker image ls
#docker run -it cdspark:latest /bin/bash


FROM maven:3.6-jdk-11
WORKDIR /opt/MavenProject
COPY src ./src
COPY pom.xml .
COPY stocks_0021daily_interpolated_full.csv /opt/MavenProject
RUN mvn clean install \
&& cd target \
&& java -cp SimilarityDetective-1.0-jar-with-dependencies.jar core.Main

#https://www.linkedin.com/pulse/dockerizing-maven-project-akshay-sharma
#docker build -t testdockerfile .
#docker run --name=testApplication -it testdockerfile:latest

#below code gives "no main manifest attribute"
#java -cp SimilarityDetective-1.0-jar-with-dependencies.jar Main
