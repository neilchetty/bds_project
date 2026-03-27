#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_CONFIG="${1:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
OUTPUT_DIR="${2:-$ROOT_DIR/work/hadoop-docker-cluster}"
IMAGE_TAG="${3:-gene2life-hadoop-cluster:3.4.3}"
PROJECT_NAME="${HADOOP_DOCKER_PROJECT:-hadoop-paper-cluster}"
COMPOSE_FILE="$OUTPUT_DIR/docker-compose.yml"
HOST_CONF_DIR="$OUTPUT_DIR/host-conf"
CLIENT_USER="${HADOOP_CLIENT_USER:-$(id -un)}"
NAMENODE_RPC_PORT="${HADOOP_DOCKER_NN_PORT:-19000}"
NAMENODE_HTTP_PORT="${HADOOP_DOCKER_NN_HTTP_PORT:-19870}"
YARN_SCHEDULER_PORT="${HADOOP_DOCKER_YARN_SCHEDULER_PORT:-18030}"
YARN_TRACKER_PORT="${HADOOP_DOCKER_YARN_TRACKER_PORT:-18031}"
YARN_RM_PORT="${HADOOP_DOCKER_YARN_RM_PORT:-18032}"
YARN_ADMIN_PORT="${HADOOP_DOCKER_YARN_ADMIN_PORT:-18033}"
YARN_WEB_PORT="${HADOOP_DOCKER_YARN_WEB_PORT:-18088}"
ACCESS_HOST="${HADOOP_DOCKER_ACCESS_HOST:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
ACCESS_HOST="${ACCESS_HOST:-localhost}"
MASTER_NODE_MEMORY_MB="${HADOOP_DOCKER_MASTER_MEMORY_MB:-2048}"
MASTER_NODE_VCORES="${HADOOP_DOCKER_MASTER_VCORES:-2}"

mkdir -p "$OUTPUT_DIR" "$HOST_CONF_DIR"

mapfile -t CLUSTER_LINES < <(grep -v '^\s*#' "$CLUSTER_CONFIG" | grep -v '^\s*$')
if [[ "${#CLUSTER_LINES[@]}" -eq 0 ]]; then
  echo "No cluster nodes found in $CLUSTER_CONFIG" >&2
  exit 1
fi

WORKER_HOSTS=()
LABELS=()
MAX_MEMORY_MB=0
MAX_CPU_VCORES=0

for line in "${CLUSTER_LINES[@]}"; do
  IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
  WORKER_HOSTS+=("$node_id")
  if [[ ! " ${LABELS[*]} " =~ " ${cluster_id} " ]]; then
    LABELS+=("$cluster_id")
  fi
  if (( memory_mb > MAX_MEMORY_MB )); then
    MAX_MEMORY_MB="$memory_mb"
  fi
  if (( cpu_threads > MAX_CPU_VCORES )); then
    MAX_CPU_VCORES="$cpu_threads"
  fi
  mkdir -p "$OUTPUT_DIR/${node_id}-data"
done
mkdir -p "$OUTPUT_DIR/master-data"

WORKER_HOSTS_JOINED="$(IFS=,; echo "${WORKER_HOSTS[*]}")"
LABELS_JOINED="$(IFS=,; echo "${LABELS[*]}")"

{
  echo "services:"
  echo "  master:"
  echo "    image: ${IMAGE_TAG}"
  echo "    hostname: master"
  echo "    environment:"
  echo "      ROLE: master"
  echo "      MASTER_HOST: master"
  echo "      WORKER_HOSTS: ${WORKER_HOSTS_JOINED}"
  echo "      CLUSTER_LABELS: ${LABELS_JOINED}"
  echo "      YARN_MAX_MEMORY_MB: ${MAX_MEMORY_MB}"
  echo "      YARN_MAX_VCORES: ${MAX_CPU_VCORES}"
  echo "      NODE_MEMORY_MB: ${MASTER_NODE_MEMORY_MB}"
  echo "      NODE_CPU_VCORES: ${MASTER_NODE_VCORES}"
  echo "    ports:"
  echo "      - \"${NAMENODE_RPC_PORT}:9000\""
  echo "      - \"${NAMENODE_HTTP_PORT}:9870\""
  echo "      - \"${YARN_SCHEDULER_PORT}:8030\""
  echo "      - \"${YARN_TRACKER_PORT}:8031\""
  echo "      - \"${YARN_RM_PORT}:8032\""
  echo "      - \"${YARN_ADMIN_PORT}:8033\""
  echo "      - \"${YARN_WEB_PORT}:8088\""
  echo "    volumes:"
  echo "      - ${OUTPUT_DIR}/master-data:/data"

  for line in "${CLUSTER_LINES[@]}"; do
    IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
    echo "  ${node_id}:"
    echo "    image: ${IMAGE_TAG}"
    echo "    hostname: ${node_id}"
    echo "    depends_on:"
    echo "      - master"
    echo "    environment:"
    echo "      ROLE: worker"
    echo "      MASTER_HOST: master"
    echo "      WORKER_HOSTS: ${WORKER_HOSTS_JOINED}"
    echo "      CLUSTER_LABELS: ${LABELS_JOINED}"
    echo "      YARN_MAX_MEMORY_MB: ${MAX_MEMORY_MB}"
    echo "      YARN_MAX_VCORES: ${MAX_CPU_VCORES}"
    echo "      NODE_MEMORY_MB: ${memory_mb}"
    echo "      NODE_CPU_VCORES: ${cpu_threads}"
    echo "    volumes:"
    echo "      - ${OUTPUT_DIR}/${node_id}-data:/data"
    echo "    mem_limit: ${memory_mb}m"
    echo "    cpus: '${cpu_threads}'"
    if [[ -n "${cpu_set:-}" ]]; then
      echo "    cpuset: \"${cpu_set}\""
    fi
  done
} > "$COMPOSE_FILE"

cat > "$HOST_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${ACCESS_HOST}:${NAMENODE_RPC_PORT}</value>
  </property>
</configuration>
EOF

cat > "$HOST_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>false</value>
  </property>
</configuration>
EOF

cat > "$HOST_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.staging.root.dir</name>
    <value>/user/${CLIENT_USER}/.staging</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.staging-dir</name>
    <value>/user/${CLIENT_USER}/.staging</value>
  </property>
</configuration>
EOF

cat > "$HOST_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>${ACCESS_HOST}:${YARN_SCHEDULER_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>${ACCESS_HOST}:${YARN_TRACKER_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>${ACCESS_HOST}:${YARN_RM_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>${ACCESS_HOST}:${YARN_ADMIN_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${ACCESS_HOST}</value>
  </property>
  <property>
    <name>yarn.webapp.address</name>
    <value>${ACCESS_HOST}:${YARN_WEB_PORT}</value>
  </property>
</configuration>
EOF

echo "Generated compose file at $COMPOSE_FILE"
echo "Generated host Hadoop config at $HOST_CONF_DIR"
echo "Docker Hadoop endpoints:"
echo "  HDFS RPC: hdfs://${ACCESS_HOST}:${NAMENODE_RPC_PORT}"
echo "  NameNode UI: http://${ACCESS_HOST}:${NAMENODE_HTTP_PORT}"
echo "  YARN RM: ${ACCESS_HOST}:${YARN_RM_PORT}"
echo "  YARN scheduler: ${ACCESS_HOST}:${YARN_SCHEDULER_PORT}"
echo "  YARN tracker: ${ACCESS_HOST}:${YARN_TRACKER_PORT}"
echo "  YARN admin: ${ACCESS_HOST}:${YARN_ADMIN_PORT}"
echo "  YARN UI: http://${ACCESS_HOST}:${YARN_WEB_PORT}"
