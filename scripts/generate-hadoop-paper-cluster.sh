#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_CONFIG="${1:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
OUTPUT_DIR="${2:-$ROOT_DIR/work/hadoop-paper-cluster}"
MAX_NODES="${MAX_NODES:-0}"
IMAGE_TAG="${IMAGE_TAG:-gene2life-hadoop-cluster:3.4.3}"
MASTER_SERVICE="${MASTER_SERVICE:-master}"
CLIENT_USER="${CLIENT_USER:-$(id -un)}"

mkdir -p "$OUTPUT_DIR"

SELECTED_CSV="$OUTPUT_DIR/selected-nodes.csv"

awk -F, -v max_nodes="$MAX_NODES" '
  /^[[:space:]]*#/ || NF == 0 { next }
  {
    cluster=$1
    idx=count[cluster]++
    line[cluster,idx]=$0
    if (!(cluster in seen)) {
      seen[cluster]=1
      order[++clusterCount]=cluster
    }
    total++
  }
  END {
    if (max_nodes <= 0 || max_nodes >= total) {
      for (i=1; i<=clusterCount; i++) {
        cluster=order[i]
        for (j=0; j<count[cluster]; j++) {
          print line[cluster,j]
        }
      }
      exit
    }
    picked=0
    level=0
    while (picked < max_nodes) {
      added=0
      for (i=1; i<=clusterCount; i++) {
        cluster=order[i]
        if (level < count[cluster]) {
          print line[cluster,level]
          picked++
          added=1
          if (picked >= max_nodes) {
            break
          }
        }
      }
      if (!added) {
        break
      }
      level++
    }
  }
' "$CLUSTER_CONFIG" > "$SELECTED_CSV"

WORKER_HOSTS="$(cut -d, -f2 "$SELECTED_CSV" | paste -sd, -)"
LABELS="$(cut -d, -f1 "$SELECTED_CSV" | awk '!seen[$0]++ {labels = labels (labels ? "," : "") $0} END {print labels}')"
YARN_MAX_MEMORY_MB="$(cut -d, -f5 "$SELECTED_CSV" | sort -n | tail -1)"
YARN_MAX_VCORES="$(cut -d, -f3 "$SELECTED_CSV" | sort -n | tail -1)"
NODE_COUNT="$(wc -l < "$SELECTED_CSV" | tr -d ' ')"

COMPOSE_FILE="$OUTPUT_DIR/docker-compose.yml"
CLIENT_CONF_DIR="$OUTPUT_DIR/client-conf"
mkdir -p "$CLIENT_CONF_DIR"

cat > "$COMPOSE_FILE" <<EOF
services:
  ${MASTER_SERVICE}:
    image: ${IMAGE_TAG}
    hostname: ${MASTER_SERVICE}
    container_name: gene2life-hadoop-master
    environment:
      ROLE: master
      MASTER_HOST: ${MASTER_SERVICE}
      WORKER_HOSTS: ${WORKER_HOSTS}
      CLUSTER_LABELS: ${LABELS}
      YARN_MAX_MEMORY_MB: "${YARN_MAX_MEMORY_MB}"
      YARN_MAX_VCORES: "${YARN_MAX_VCORES}"
      NODE_MEMORY_MB: "${YARN_MAX_MEMORY_MB}"
      NODE_CPU_VCORES: "${YARN_MAX_VCORES}"
    ports:
      - "9000:9000"
      - "9870:9870"
      - "8088:8088"
      - "8030:8030"
      - "8031:8031"
      - "8032:8032"
      - "8033:8033"
    volumes:
      - ${OUTPUT_DIR}/master-data:/data
EOF

while IFS=, read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set; do
  cat >> "$COMPOSE_FILE" <<EOF
  ${node_id}:
    image: ${IMAGE_TAG}
    hostname: ${node_id}
    container_name: gene2life-${node_id}
    environment:
      ROLE: worker
      MASTER_HOST: ${MASTER_SERVICE}
      WORKER_HOSTS: ${WORKER_HOSTS}
      CLUSTER_LABELS: ${LABELS}
      YARN_MAX_MEMORY_MB: "${YARN_MAX_MEMORY_MB}"
      YARN_MAX_VCORES: "${YARN_MAX_VCORES}"
      NODE_MEMORY_MB: "${memory_mb}"
      NODE_CPU_VCORES: "${cpu_threads}"
    depends_on:
      - ${MASTER_SERVICE}
    cpus: "${cpu_threads}.0"
    mem_limit: "${memory_mb}m"
    volumes:
      - ${OUTPUT_DIR}/${node_id}-data:/data
EOF
  if [[ -n "${cpu_set}" ]]; then
    cat >> "$COMPOSE_FILE" <<EOF
    cpuset: "${cpu_set}"
EOF
  fi
done < "$SELECTED_CSV"

cat > "$CLIENT_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

cat > "$CLIENT_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>localhost:9000</value>
  </property>
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

cat > "$CLIENT_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.staging.root.dir</name>
    <value>/user/${CLIENT_USER}/.staging</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
EOF

cat > "$CLIENT_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>localhost:8032</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>localhost:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>localhost:8031</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>localhost:8033</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>256</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.node-labels.enabled</name>
    <value>true</value>
  </property>
</configuration>
EOF

cat > "$OUTPUT_DIR/cluster.env" <<EOF
export HADOOP_CONF_DIR=${CLIENT_CONF_DIR}
export HADOOP_FS_DEFAULT=hdfs://localhost:9000
export HADOOP_YARN_RM=localhost:8032
export HADOOP_FRAMEWORK_NAME=yarn
export HADOOP_ENABLE_NODE_LABELS=true
export GENE2LIFE_HADOOP_CLUSTER_DIR=${OUTPUT_DIR}
EOF

cat > "$OUTPUT_DIR/node-labels.txt" <<EOF
${LABELS}
EOF

cat > "$OUTPUT_DIR/node-mapping.txt" <<EOF
$(awk -F, '{printf "%s%s=%s", (NR == 1 ? "" : " "), $2, $1}' "$SELECTED_CSV")
EOF

echo "Generated Hadoop paper cluster files under $OUTPUT_DIR"
echo "Compose file: $COMPOSE_FILE"
echo "Client Hadoop conf: $CLIENT_CONF_DIR"
echo "Selected worker count: $NODE_COUNT"
