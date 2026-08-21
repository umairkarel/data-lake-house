FROM flink:1.20.0

ARG HADOOP_VERSION=3.4.0
ARG FLINK_VERSION=1.20.0
ARG ICEBERG_VERSION=1.8.1
ARG HADOOP_UBER_VERSION=2.8.3-10.0
ARG POSTGRESQL_VERSION=42.7.1
ARG AWS_VERSION=2.31.0
ARG KAFKA_CONNECTOR_VERSION=3.4.0

# ---------------------------------------------------------------------------
# System tools
# ---------------------------------------------------------------------------
RUN apt update && apt install -y nano iputils-ping curl wget python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# MinIO client (mc) — handy for bucket bootstrap from inside the container
# ---------------------------------------------------------------------------
RUN curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# ---------------------------------------------------------------------------
# S3 / Hadoop JARs  (required for Flink ↔ MinIO via s3a://)
# ---------------------------------------------------------------------------
RUN curl -o /opt/flink/lib/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar && \
    chown flink:flink /opt/flink/lib/flink-s3-fs-hadoop-${FLINK_VERSION}.jar

RUN curl -o /opt/flink/lib/hadoop-common-${HADOOP_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar && \
    chown -R flink:flink /opt/flink/lib/hadoop-common-${HADOOP_VERSION}.jar

RUN curl -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar && \
    chown -R flink:flink /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar

# ---------------------------------------------------------------------------
# AWS SDK v2 bundle — needed for s3a:// path-style access to MinIO
# ---------------------------------------------------------------------------
RUN curl -o /opt/flink/lib/bundle-${AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_VERSION}/bundle-${AWS_VERSION}.jar && \
    chown -R flink:flink /opt/flink/lib/bundle-${AWS_VERSION}.jar

# ---------------------------------------------------------------------------
# Apache Iceberg — Flink runtime bundle (contains NessieCatalog + IcebergSink)
# ---------------------------------------------------------------------------
RUN curl -o /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar && \
    chown -R flink:flink /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar

# ---------------------------------------------------------------------------
# Kafka connector — for reading from Kafka in Flink SQL / DataStream
# ---------------------------------------------------------------------------
RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}-1.20/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar && \
    chown -R flink:flink /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar

RUN curl -o /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}-1.20/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar && \
    chown -R flink:flink /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.20.jar

RUN curl -o /opt/flink/lib/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    chown -R flink:flink /opt/flink/lib/kafka-clients-3.4.0.jar

# ---------------------------------------------------------------------------
# PostgreSQL JDBC — in separate dir for optional use
# ---------------------------------------------------------------------------
RUN mkdir -p /opt/flink/lib/jdbc
RUN curl -o /opt/flink/lib/jdbc/postgresql-${POSTGRESQL_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRESQL_VERSION}.jar && \
    chown -R flink:flink /opt/flink/lib/jdbc/postgresql-${POSTGRESQL_VERSION}.jar

# ---------------------------------------------------------------------------
# PyFlink — install apache-flink Python package matching the Flink version
# ---------------------------------------------------------------------------
RUN pip3 install --no-cache-dir apache-flink==${FLINK_VERSION} && \
    pip3 install --no-cache-dir pyiceberg[s3fs,nessie]==0.8.1 pyyaml

# ---------------------------------------------------------------------------
# Jobs directory — PyFlink job scripts will be mounted/copied here
# ---------------------------------------------------------------------------
RUN mkdir -p /opt/flink/jobs && chown -R flink:flink /opt/flink/jobs

# Verify PyFlink is importable
RUN python3 -c "from pyflink.datastream import StreamExecutionEnvironment; print('PyFlink OK')"
