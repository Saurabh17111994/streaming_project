FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.20
WORKDIR /opt/flink-jobs
COPY --from=build /src/target/compute.jar /opt/flink-jobs/compute.jar
COPY submit-jobs.sh /opt/flink-jobs/submit-jobs.sh
RUN chmod +x /opt/flink-jobs/submit-jobs.sh
# JobManager REST is used to submit the three jobs on start.
CMD ["/opt/flink-jobs/submit-jobs.sh"]
