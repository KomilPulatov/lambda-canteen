FROM flink:1.19-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    apache-flink==1.19.1 \
    redis==5.0.4

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

USER flink
