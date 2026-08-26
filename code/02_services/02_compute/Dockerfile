# Native Flink split: the image is the PLATFORM (Flink + launcher) only.
# The job jar is a separate artifact — built on the host (`mvn package`) and
# mounted at runtime (compose volume: target/compute.jar ->
# /opt/flink-jobs/compute.jar) or submitted via `make rollout-savepoint`.
# NEVER bake the jar or compile code inside this image: that couples every
# code change to an image rebuild (the anti-pattern this replaces).
FROM flink:2.2.1-scala_2.12-java17
WORKDIR /opt/flink-jobs
COPY --chmod=+x 02_services/02_compute/submit-jobs.sh /opt/flink-jobs/submit-jobs.sh
# The compute.jar is NOT copied here — it is mounted by compose (volume) or
# supplied by rollout-savepoint at submit time. The launcher reads it from
# /opt/flink-jobs/compute.jar (the mount point); the compose volume makes the
# host-built jar available without an image rebuild.
CMD ["/opt/flink-jobs/submit-jobs.sh"]
