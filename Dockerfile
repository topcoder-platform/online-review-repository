FROM maven:3.8.6-openjdk-18 as build
COPY src /home/tc-or-grpc-server/src
COPY pom.xml /home/tc-or-grpc-server
COPY libs /home/tc-or-grpc-server/libs
RUN mvn install:install-file -Dfile=/home/tc-or-grpc-server/libs/tc-dal-or-proto-1.0-SNAPSHOT.jar -DgroupId=com.topcoder -DartifactId=tc-dal-or-proto -Dversion=1.0-SNAPSHOT -Dpackaging=jar
RUN rm -r /home/tc-or-grpc-server/libs
RUN mvn -f /home/tc-or-grpc-server/pom.xml clean package

FROM openjdk:18.0.2.1-oracle
COPY --from=build /home/tc-or-grpc-server/target/or-repository-0.0.1-SNAPSHOT.jar /usr/local/lib/or-repository.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/usr/local/lib/or-repository.jar"]