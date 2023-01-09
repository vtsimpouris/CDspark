#run this first, from terminal
#sudo chmod 0777 /var/run/docker.sock
FROM maven:3.8.6-openjdk-11 AS build
FROM openjdk:11
COPY src /tmp
#ADD src/ /tmp
#WORKDIR /tmp
ENTRYPOINT ["mvn compile"]
ENTRYPOINT ["java","Main"]