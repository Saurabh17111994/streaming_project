FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml /workspace/pom.xml
COPY common /workspace/common
COPY 02_services/06_execution_gateway/pom.xml /workspace/02_services/06_execution_gateway/pom.xml
RUN mvn -q -f /workspace/pom.xml -pl 02_services/06_execution_gateway -am dependency:go-offline -DskipTests || true
COPY 02_services/06_execution_gateway/src /workspace/02_services/06_execution_gateway/src
RUN mvn -q -f /workspace/pom.xml -pl 02_services/06_execution_gateway -am package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/02_services/06_execution_gateway/target/execution-gateway.jar /app/execution-gateway.jar
USER 10001:10001
ENTRYPOINT ["java", "-jar", "/app/execution-gateway.jar"]
