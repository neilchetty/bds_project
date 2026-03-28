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
PREFLIGHT_DIR="${PREFLIGHT_DIR:-$SESSION_DIR/preflight}"
SUBMISSION_MODE="${SUBMISSION_MODE:-true}"
FULL_SWEEP="${FULL_SWEEP:-false}"
if [[ "$FULL_SWEEP" == "true" ]]; then
  SUBMISSION_MODE="false"
fi
DEFAULT_PROFILE="medium"
DEFAULT_COMPARE_ROUNDS="4"
DEFAULT_TRAINING_WARMUP_RUNS="1"
DEFAULT_TRAINING_MEASURE_RUNS="3"
if [[ "$SUBMISSION_MODE" == "true" ]]; then
  DEFAULT_PROFILE="small"
  DEFAULT_COMPARE_ROUNDS="1"
  DEFAULT_TRAINING_WARMUP_RUNS="0"
  DEFAULT_TRAINING_MEASURE_RUNS="1"
fi
PROFILE="${PROFILE:-$DEFAULT_PROFILE}"
DEFAULT_WORKFLOWS="gene2life avianflu_small epigenomics"
if [[ "$SUBMISSION_MODE" == "true" ]]; then
  DEFAULT_WORKFLOWS="gene2life avianflu_small"
fi
WORKFLOWS="${WORKFLOWS:-$DEFAULT_WORKFLOWS}"
NODE_COUNTS="${NODE_COUNTS:-2 4 12 28}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-$DEFAULT_COMPARE_ROUNDS}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-$DEFAULT_TRAINING_WARMUP_RUNS}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-$DEFAULT_TRAINING_MEASURE_RUNS}"
EXECUTOR="${EXECUTOR:-hadoop}"
DOCKER_IMAGE="${DOCKER_IMAGE:-gene2life-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-gene2life-hadoop-cluster:3.4.3}"
PAPER_CLUSTER_CONFIG="${PAPER_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
DENSE_CLUSTER_CONFIG="${DENSE_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-dense-28.csv}"
GENE2LIFE_JAVA_OPTS="${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}"
export BASE_WORK_DIR LOG_DIR HADOOP_CLUSTER_WORKDIR PROFILE WORKFLOWS NODE_COUNTS
export COMPARE_ROUNDS TRAINING_WARMUP_RUNS TRAINING_MEASURE_RUNS EXECUTOR DOCKER_IMAGE
export HADOOP_CLUSTER_IMAGE PAPER_CLUSTER_CONFIG DENSE_CLUSTER_CONFIG GENE2LIFE_JAVA_OPTS
export PREFLIGHT_DIR SUBMISSION_MODE FULL_SWEEP
export SKIP_PREBUILD=true
export HADOOP_KEEP_CLUSTER="${HADOOP_KEEP_CLUSTER:-false}"
export RUN_ALL_KEEP_CLUSTER="$HADOOP_KEEP_CLUSTER"
export HADOOP_CLUSTER_WARMUP_ON_START="${HADOOP_CLUSTER_WARMUP_ON_START:-false}"

mkdir -p "$SESSION_DIR" "$BASE_WORK_DIR" "$LOG_DIR" "$PREFLIGHT_DIR"
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

write_env_entry() {
  local key="$1"
  local value="$2"
  printf '%s=%q\n' "$key" "$value"
}

render_report() {
  local status="$1"
  local end_ts="$2"

  {
    write_env_entry "RUN_STATUS" "$status"
    write_env_entry "ROOT_DIR" "$ROOT_DIR"
    write_env_entry "SESSION_DIR" "$SESSION_DIR"
    write_env_entry "TRANSCRIPT" "$TRANSCRIPT"
    write_env_entry "REPORT_HTML" "$REPORT_HTML"
    write_env_entry "REPORT_PDF" "$REPORT_PDF"
    write_env_entry "BASE_WORK_DIR" "$BASE_WORK_DIR"
    write_env_entry "LOG_DIR" "$LOG_DIR"
    write_env_entry "HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_WORKDIR"
    write_env_entry "PREFLIGHT_DIR" "$PREFLIGHT_DIR"
    write_env_entry "SUBMISSION_MODE" "$SUBMISSION_MODE"
    write_env_entry "FULL_SWEEP" "$FULL_SWEEP"
    write_env_entry "PROFILE" "$PROFILE"
    write_env_entry "WORKFLOWS" "$WORKFLOWS"
    write_env_entry "NODE_COUNTS" "$NODE_COUNTS"
    write_env_entry "COMPARE_ROUNDS" "$COMPARE_ROUNDS"
    write_env_entry "TRAINING_WARMUP_RUNS" "$TRAINING_WARMUP_RUNS"
    write_env_entry "TRAINING_MEASURE_RUNS" "$TRAINING_MEASURE_RUNS"
    write_env_entry "EXECUTOR" "$EXECUTOR"
    write_env_entry "DOCKER_IMAGE" "$DOCKER_IMAGE"
    write_env_entry "HADOOP_CLUSTER_IMAGE" "$HADOOP_CLUSTER_IMAGE"
    write_env_entry "PAPER_CLUSTER_CONFIG" "$PAPER_CLUSTER_CONFIG"
    write_env_entry "DENSE_CLUSTER_CONFIG" "$DENSE_CLUSTER_CONFIG"
    write_env_entry "GENE2LIFE_JAVA_OPTS" "$GENE2LIFE_JAVA_OPTS"
    write_env_entry "START_TS" "$RUN_TS"
    write_env_entry "END_TS" "$end_ts"
    write_env_entry "LAST_COMMAND" "$LAST_COMMAND"
  } > "$ENV_SNAPSHOT"

  python3 - "$SESSION_DIR" "$BASE_WORK_DIR" "$LOG_DIR" "$TRANSCRIPT" "$REPORT_HTML" "$RESULT_SUMMARY" "$status" "$RUN_TS" "$end_ts" "$WORKFLOWS" "$NODE_COUNTS" <<'PY'
import html
import pathlib
import re
import sys

session_dir = pathlib.Path(sys.argv[1])
base_work_dir = pathlib.Path(sys.argv[2])
log_dir = pathlib.Path(sys.argv[3])
transcript_path = pathlib.Path(sys.argv[4])
report_html = pathlib.Path(sys.argv[5])
result_summary = pathlib.Path(sys.argv[6])
status = sys.argv[7]
start_ts = sys.argv[8]
end_ts = sys.argv[9]
workflows = [value for value in sys.argv[10].split() if value]
node_counts = [value for value in sys.argv[11].split() if value]

def read_text(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")

comparison_blocks = []
comparison_by_name = {}
summary_lines = []
for comparison in sorted(base_work_dir.glob("*/comparison.md")):
    content = read_text(comparison)
    comparison_blocks.append((comparison.parent.name, comparison, content))
    comparison_by_name[comparison.parent.name] = (comparison, content)
    match = re.search(r"WSH makespan improvement over HEFT: ([\-0-9.]+)%", content)
    improvement = match.group(1) if match else "n/a"
    summary_lines.append(f"{comparison.parent.name}: COMPLETED, WSH improvement {improvement}%")

transcript = read_text(transcript_path)
started = set(re.findall(r"Starting ([A-Za-z0-9_-]+)", transcript))
completed = set(re.findall(r"Completed ([A-Za-z0-9_-]+)", transcript))
failed = set(re.findall(r"FAILED ([A-Za-z0-9_-]+)", transcript))
current_run_path = log_dir / "current-run.txt"
current_run = read_text(current_run_path).strip() if current_run_path.exists() else ""
matrix_lines = []
for workflow in workflows:
    for node_count in node_counts:
        run_name = f"{workflow}-nodes-{node_count}"
        if run_name in comparison_by_name:
            comparison_path, content = comparison_by_name[run_name]
            match = re.search(r"WSH makespan improvement over HEFT: ([\-0-9.]+)%", content)
            improvement = match.group(1) if match else "n/a"
            matrix_lines.append(f"{run_name}: COMPLETED, WSH improvement {improvement}%, report={comparison_path}")
        elif run_name in failed:
            matrix_lines.append(f"{run_name}: FAILED")
        elif current_run == run_name or run_name in started or run_name in completed:
            matrix_lines.append(f"{run_name}: INCOMPLETE")
        else:
            matrix_lines.append(f"{run_name}: NOT_STARTED")

if matrix_lines:
    summary_lines.extend(["", "Execution matrix:"])
    summary_lines.extend(matrix_lines)
result_summary.write_text("\n".join(summary_lines).strip() + "\n", encoding="utf-8")

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
if matrix_lines:
    sections.append("<h2>Execution Matrix</h2><pre>" + html.escape("\n".join(matrix_lines)) + "</pre>")

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

  {
    echo "Overnight Hadoop Sweep Report"
    echo "Status: $status"
    echo "Started: $RUN_TS"
    echo "Finished: $end_ts"
    echo "Session: $SESSION_DIR"
    echo
    cat "$RESULT_SUMMARY"
    echo
    echo "Full Transcript"
    echo "==============="
    cat "$TRANSCRIPT"
  } > "$REPORT_TXT"
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

  run_cmd "$ROOT_DIR/scripts/submission-preflight.sh" "$PREFLIGHT_DIR"

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
