#run this first, from terminal
#sudo chmod 0777 /var/run/docker.sock
#FROM maven:3.8.6-openjdk-11 AS build
#FROM openjdk:11
#COPY src /tmp
#ADD src/ /tmp
#WORKDIR /tmp
#ENTRYPOINT ["mvn compile"]
#ENTRYPOINT ["java","Main"]
FROM maven:3.6-jdk-11
WORKDIR /opt/MavenProject
COPY src ./src
COPY pom.xml .
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable-current_amd64.deb; apt-get -fy install
RUN rm -rf google-chrome-stable_current_amd64.deb
RUN mvn clean
CMD ["mvn","clean","install

#https://www.linkedin.com/pulse/dockerizing-maven-project-akshay-sharma
#docker build -t testdockerfile .
#docker run --name=testApplication -it testdockerfile:latest