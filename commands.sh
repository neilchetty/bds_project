#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_TS="${RUN_TS:-$(date '+%Y%m%d-%H%M%S')}"
SESSION_DIR="${1:-${RUNNER_SESSION_DIR:-$ROOT_DIR/work/overnight-run-$RUN_TS}}"
SESSION_DIR="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$SESSION_DIR")"
TRANSCRIPT="$SESSION_DIR/transcript.log"
REPORT_HTML="$SESSION_DIR/report.html"
REPORT_PDF="$SESSION_DIR/report.pdf"
REPORT_TXT="$SESSION_DIR/report.txt"
RESULT_SUMMARY="$SESSION_DIR/result-summary.txt"
ENV_SNAPSHOT="$SESSION_DIR/run.env"
BASE_WORK_DIR="${BASE_WORK_DIR:-$SESSION_DIR/workspaces}"
LOG_DIR="${LOG_DIR:-$SESSION_DIR/sweep-logs}"
HADOOP_CLUSTER_WORKDIR="${HADOOP_CLUSTER_WORKDIR:-$SESSION_DIR/hadoop-docker-cluster}"
PROFILE="${PROFILE:-medium}"
WORKFLOWS="${WORKFLOWS:-gene2life avianflu_small epigenomics}"
NODE_COUNTS="${NODE_COUNTS:-2 4 12 28}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-4}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
EXECUTOR="${EXECUTOR:-hadoop}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-gene2life-hadoop-cluster:3.4.3}"
PAPER_CLUSTER_CONFIG="${PAPER_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
DENSE_CLUSTER_CONFIG="${DENSE_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-dense-28.csv}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
export BASE_WORK_DIR LOG_DIR HADOOP_CLUSTER_WORKDIR PROFILE WORKFLOWS NODE_COUNTS
export COMPARE_ROUNDS TRAINING_WARMUP_RUNS TRAINING_MEASURE_RUNS EXECUTOR DOCKER_IMAGE
export HADOOP_CLUSTER_IMAGE PAPER_CLUSTER_CONFIG DENSE_CLUSTER_CONFIG GENE2LIFE_JAVA_OPTS
export SKIP_PREBUILD=true
export HADOOP_KEEP_CLUSTER="${HADOOP_KEEP_CLUSTER:-false}"
export RUN_ALL_KEEP_CLUSTER="$HADOOP_KEEP_CLUSTER"
export HADOOP_CLUSTER_WARMUP_ON_START="${HADOOP_CLUSTER_WARMUP_ON_START:-false}"

mkdir -p "$SESSION_DIR" "$BASE_WORK_DIR" "$LOG_DIR"
touch "$TRANSCRIPT"
exec > >(tee -a "$TRANSCRIPT") 2>&1

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

LAST_COMMAND=""

run_cmd() {
  LAST_COMMAND="$*"
  log "COMMAND: $*"
  "$@"
}

run_shell() {
  LAST_COMMAND="$*"
  log "COMMAND: $*"
  bash -lc "$*"
}

render_report() {
  local status="$1"
  local end_ts="$2"

  {
    echo "RUN_STATUS=$status"
    echo "ROOT_DIR=$ROOT_DIR"
    echo "SESSION_DIR=$SESSION_DIR"
    echo "TRANSCRIPT=$TRANSCRIPT"
    echo "REPORT_HTML=$REPORT_HTML"
    echo "REPORT_PDF=$REPORT_PDF"
    echo "BASE_WORK_DIR=$BASE_WORK_DIR"
    echo "LOG_DIR=$LOG_DIR"
    echo "HADOOP_CLUSTER_WORKDIR=$HADOOP_CLUSTER_WORKDIR"
    echo "PROFILE=$PROFILE"
    echo "WORKFLOWS=$WORKFLOWS"
    echo "NODE_COUNTS=$NODE_COUNTS"
    echo "COMPARE_ROUNDS=$COMPARE_ROUNDS"
    echo "TRAINING_WARMUP_RUNS=$TRAINING_WARMUP_RUNS"
    echo "TRAINING_MEASURE_RUNS=$TRAINING_MEASURE_RUNS"
    echo "EXECUTOR=$EXECUTOR"
    echo "DOCKER_IMAGE=$DOCKER_IMAGE"
    echo "HADOOP_CLUSTER_IMAGE=$HADOOP_CLUSTER_IMAGE"
    echo "PAPER_CLUSTER_CONFIG=$PAPER_CLUSTER_CONFIG"
    echo "DENSE_CLUSTER_CONFIG=$DENSE_CLUSTER_CONFIG"
    echo "GENE2LIFE_JAVA_OPTS=$GENE2LIFE_JAVA_OPTS"
    echo "START_TS=$RUN_TS"
    echo "END_TS=$end_ts"
    echo "LAST_COMMAND=$LAST_COMMAND"
  } > "$ENV_SNAPSHOT"

  python3 - "$SESSION_DIR" "$BASE_WORK_DIR" "$TRANSCRIPT" "$REPORT_HTML" "$RESULT_SUMMARY" "$status" "$RUN_TS" "$end_ts" <<'PY'
import html
import pathlib
import re
import sys

session_dir = pathlib.Path(sys.argv[1])
base_work_dir = pathlib.Path(sys.argv[2])
transcript_path = pathlib.Path(sys.argv[3])
report_html = pathlib.Path(sys.argv[4])
result_summary = pathlib.Path(sys.argv[5])
status = sys.argv[6]
start_ts = sys.argv[7]
end_ts = sys.argv[8]

def read_text(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")

comparison_blocks = []
summary_lines = []
for comparison in sorted(base_work_dir.glob("*/comparison.md")):
    content = read_text(comparison)
    comparison_blocks.append((comparison.parent.name, comparison, content))
    match = re.search(r"WSH makespan improvement over HEFT: ([\-0-9.]+)%", content)
    improvement = match.group(1) if match else "n/a"
    summary_lines.append(f"{comparison.parent.name}: WSH improvement {improvement}%")

result_summary.write_text("\n".join(summary_lines) + ("\n" if summary_lines else ""), encoding="utf-8")

transcript = read_text(transcript_path)

sections = []
sections.append(
    f"""
    <h1>Overnight Hadoop Sweep Report</h1>
    <p><strong>Status:</strong> {html.escape(status)}</p>
    <p><strong>Started:</strong> {html.escape(start_ts)}</p>
    <p><strong>Finished:</strong> {html.escape(end_ts)}</p>
    <p><strong>Workspace:</strong> {html.escape(str(session_dir))}</p>
    <p><strong>Result summary:</strong> {html.escape(str(result_summary))}</p>
    """
)

if summary_lines:
    sections.append("<h2>Result Summary</h2><pre>" + html.escape("\n".join(summary_lines)) + "</pre>")

sections.append("<h2>Comparison Reports</h2>")
if comparison_blocks:
    for name, path, content in comparison_blocks:
        sections.append(
            "<h3>" + html.escape(name) + "</h3>"
            + "<p><strong>Path:</strong> " + html.escape(str(path)) + "</p>"
            + "<pre>" + html.escape(content) + "</pre>"
        )
else:
    sections.append("<p>No comparison reports were found.</p>")

sections.append("<h2>Full Command Transcript</h2>")
sections.append("<pre>" + html.escape(transcript) + "</pre>")

report_html.write_text(
    """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Overnight Hadoop Sweep Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; color: #111; }
    h1, h2, h3 { margin-bottom: 0.3em; }
    p { margin: 0.2em 0 0.6em 0; }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      background: #f5f5f5;
      border: 1px solid #ddd;
      padding: 10px;
      font-size: 11px;
      line-height: 1.35;
    }
  </style>
</head>
<body>
"""
        + "\n".join(sections)
        + "\n</body>\n</html>\n",
        encoding="utf-8",
    )
PY

  cp "$TRANSCRIPT" "$REPORT_TXT"
  if command -v wkhtmltopdf >/dev/null 2>&1; then
    if ! wkhtmltopdf --quiet --orientation Landscape "$REPORT_HTML" "$REPORT_PDF"; then
      log "WARNING: wkhtmltopdf failed; HTML and text reports were still generated"
    fi
  else
    log "WARNING: wkhtmltopdf is not installed; skipping PDF generation"
  fi
}

cleanup_runtime() {
  log "Cleaning up repo-owned runtime"
  "$ROOT_DIR/scripts/cleanup-project-runtime.sh" docker-nodes || true
  if [[ "$HADOOP_KEEP_CLUSTER" != "true" ]]; then
    HADOOP_CLUSTER_WORKDIR="$HADOOP_CLUSTER_WORKDIR" \
      HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
      CLUSTER_CONFIG="$PAPER_CLUSTER_CONFIG" \
      "$ROOT_DIR/scripts/cleanup-project-runtime.sh" hadoop-cluster || true
  else
    log "Leaving project Docker Hadoop cluster running because HADOOP_KEEP_CLUSTER=true"
  fi
  if [[ "$HADOOP_KEEP_CLUSTER" != "true" && -f "$HADOOP_CLUSTER_WORKDIR/docker-compose.yml" ]]; then
    docker compose -p "${HADOOP_DOCKER_PROJECT:-hadoop-paper-cluster}" -f "$HADOOP_CLUSTER_WORKDIR/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}

main() {
  log "Overnight run session directory: $SESSION_DIR"
  run_shell "pwd"
  run_shell "fastfetch --logo none || true"
  run_shell "uname -a"
  run_shell "nproc"
  run_shell "lscpu"
  run_shell "free -h"
  run_shell "df -h"
  run_shell "java -version"
  run_shell "mvn -version"
  run_shell "docker info --format '{{.NCPU}} CPUs / {{.MemTotal}} bytes / cgroup={{.CgroupVersion}} / driver={{.Driver}}'"
  run_shell "jps || true"

  run_cmd "$ROOT_DIR/scripts/build.sh"
  run_cmd "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
  run_cmd "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$HADOOP_CLUSTER_IMAGE"

  run_cmd "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$PAPER_CLUSTER_CONFIG" "$DOCKER_IMAGE"
  run_cmd "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$DENSE_CLUSTER_CONFIG" "$DOCKER_IMAGE"

  run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" up "$PAPER_CLUSTER_CONFIG" "$HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_IMAGE"
  run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" health "$PAPER_CLUSTER_CONFIG" "$HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_IMAGE"
  run_cmd "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" validate "$PAPER_CLUSTER_CONFIG" "$HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_IMAGE"

  run_cmd "$ROOT_DIR/scripts/run-all-workflow-sweeps.sh"

  log "Collecting produced comparison reports"
  run_shell "find '$BASE_WORK_DIR' -maxdepth 2 -name comparison.md | sort"
  log "Overnight run completed successfully"
}

finish() {
  local exit_code="$?"
  local status="SUCCESS"
  if [[ "$exit_code" -ne 0 ]]; then
    status="FAILED"
    log "Run failed while executing: ${LAST_COMMAND:-unknown command}"
  fi
  end_ts="$(date '+%F %T')"
  cleanup_runtime
  render_report "$status" "$end_ts"
  log "HTML report: $REPORT_HTML"
  log "PDF report: $REPORT_PDF"
  log "Transcript: $TRANSCRIPT"
  log "Result summary: $RESULT_SUMMARY"
  exit "$exit_code"
}

trap finish EXIT

main
