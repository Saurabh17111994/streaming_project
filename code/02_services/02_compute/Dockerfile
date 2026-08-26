FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml /workspace/pom.xml
COPY common /workspace/common
COPY 02_services /workspace/02_services
# 02_services/02_compute is NOT a reactor module (parent pom lists common,
# 01_ingestion, 06_execution_gateway; compute declares the parent but is
# built standalone). Install parent + common to the local repo, then build
# compute with its own pom (relativePath ../../pom.xml resolves).
RUN mvn -q -f /workspace/pom.xml -pl common -am install -DskipTests
RUN cd /workspace/02_services/02_compute && mvn -q -DskipTests package

FROM flink:2.2.1-scala_2.12-java17
WORKDIR /opt/flink-jobs
COPY --from=build /workspace/02_services/02_compute/target/compute.jar /opt/flink-jobs/compute.jar
COPY --chmod=+x 02_services/02_compute/submit-jobs.sh /opt/flink-jobs/submit-jobs.sh
# JobManager REST is used to submit the Flink jobs on start.
CMD ["/opt/flink-jobs/submit-jobs.sh"]
