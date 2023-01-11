#instructions here
#run this first, from terminal
#sudo chmod 0777 /var/run/docker.sock
FROM maven:3.6-jdk-11@sha256:1d29ccf46ef2a5e64f7de3d79a63f9bcffb4dc56be0ae3daed5ca5542b38aa2d
WORKDIR /opt/MavenProject
COPY src ./src
COPY pom.xml .
#RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
#RUN dpkg -i google-chrome-stable-current_amd64.deb; apt-get -fy install
#RUN rm -rf google-chrome-stable_current_amd64.deb
RUN mvn clean
#tests fail (delete them?)
CMD ["mvn","clean","install"]
RUN java -cp SimilarityDetective-1.0-jar-with-dependencies.jar core.Main

#https://www.linkedin.com/pulse/dockerizing-maven-project-akshay-sharma
#docker build -t testdockerfile .
#docker run --name=testApplication -it testdockerfile:latest

#below code gives "no main manifest attribute"
#java -cp SimilarityDetective-1.0-jar-with-dependencies.jar Main
