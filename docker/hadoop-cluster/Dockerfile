FROM eclipse-temurin:17-jre

ARG HADOOP_VERSION=3.4.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl procps netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tgz \
    && tar -xzf /tmp/hadoop.tgz -C /opt \
    && ln -s "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
    && rm -f /tmp/hadoop.tgz

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/java/openjdk/bin:$PATH

COPY docker/hadoop-cluster/entrypoint.sh /usr/local/bin/hadoop-cluster-entrypoint.sh

RUN chmod +x /usr/local/bin/hadoop-cluster-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/hadoop-cluster-entrypoint.sh"]
