FROM flink:1.20.1-scala_2.12-java17
USER root
ARG KAFKA_CONNECTOR_SHA512=b744f0f3c418a45cf23ac72d477f5f8ee73f8f96a5b733b21a953a0207facd52c5495383eaad2416dbffc17e9fa5b53b3cc49e70c3cb7a2ead38a7ecac0211df
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
 && echo "${KAFKA_CONNECTOR_SHA512}  /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar" | sha512sum -c -
COPY flink/streaming.sql /opt/flink/usrlib/streaming.sql
USER flink
