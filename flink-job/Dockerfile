FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -Djava.net.preferIPv4Stack=true

FROM flink:1.20-java11
COPY --from=build /app/target/taxi-processor-1.0.jar /opt/flink/usrlib/