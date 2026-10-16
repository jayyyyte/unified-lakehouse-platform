# MOSAIC Spark: apache/spark 3.5.1 plus exactly the jars the Bronze stream needs.
#
# Baked in rather than fetched with `--packages` at submit time, so a demo does not
# depend on Maven Central being reachable and every start resolves the same bytes.
#
# Iceberg is pinned to 1.5.2 because that is what Trino 451 embeds (verified from
# /usr/lib/trino/plugin/iceberg in the trino image). Spark writes the tables and Trino
# reads them; keeping writer and reader on one Iceberg version keeps the metadata
# format symmetric instead of hoping newer-writes-older-reads holds.
FROM apache/spark:3.5.1

ARG ICEBERG=1.5.2
ARG SPARK=3.5.1
ARG SCALA=2.12
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Iceberg's Spark runtime bundles the Nessie catalog client; the AWS bundle provides
# S3FileIO for MinIO. The four Kafka jars are spark-sql-kafka and its transitive
# closure minus what Spark already ships (lz4, snappy, zstd, jsr305).
RUN set -eux; cd /opt/spark/jars; \
    for p in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA}/${ICEBERG}/iceberg-spark-runtime-3.5_${SCALA}-${ICEBERG}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar \
      org/apache/spark/spark-sql-kafka-0-10_${SCALA}/${SPARK}/spark-sql-kafka-0-10_${SCALA}-${SPARK}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_${SCALA}/${SPARK}/spark-token-provider-kafka-0-10_${SCALA}-${SPARK}.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ; \
    do curl -fsSL -O "${MAVEN}/${p}"; done; \
    chmod 644 /opt/spark/jars/*.jar

# The streaming checkpoint lives on a named volume mounted here. A fresh named volume
# copies the ownership of the directory it is mounted over, so creating it now as the
# spark user is what lets uid 185 write its checkpoint later.
RUN mkdir -p /opt/mosaic/state /opt/spark/conf && chown -R spark:spark /opt/mosaic/state

USER spark
