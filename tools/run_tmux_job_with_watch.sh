#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <session-name> <latest-file> <command...>" >&2
  exit 2
fi

SESSION_NAME="$1"
LATEST_FILE="$2"
shift 2
CMD="$*"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p artifacts/run artifacts/logs

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "ERROR tmux session already exists: $SESSION_NAME" >&2
  exit 1
fi

LOG="artifacts/logs/${SESSION_NAME}_$(date -u +%Y%m%dT%H%M%SZ).log"
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DONE_MARKER="artifacts/run/${SESSION_NAME}_done"
NOTIFIED_MARKER="artifacts/run/${SESSION_NAME}_notified_done"
RUNNER="artifacts/run/${SESSION_NAME}.runner.sh"
rm -f "$DONE_MARKER" "$NOTIFIED_MARKER"

cat > "$LATEST_FILE" <<EOF
watch=1
session=$SESSION_NAME
log=$LOG
started=$STARTED
status=running
cmd=$CMD
EOF

cat > "$RUNNER" <<EOF
#!/usr/bin/env bash
set -o pipefail
cd "${ROOT}"
echo "log=${LOG}"
echo "started=${STARTED}"
${CMD}
rc=\$?
finished=\$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf 'watch=1\nsession=%s\nlog=%s\nstarted=%s\nfinished=%s\nstatus=done\nexit_code=%s\ncmd=%s\n' \
  '${SESSION_NAME}' '${LOG}' '${STARTED}' "\$finished" "\$rc" '${CMD}' > '${LATEST_FILE}'
touch '${DONE_MARKER}'
exit \$rc
EOF
chmod +x "$RUNNER"

tmux new-session -d -s "$SESSION_NAME" -x 160 -y 48 "bash '$ROOT/$RUNNER' 2>&1 | tee '$ROOT/$LOG'"

echo "STARTED_TMUX $SESSION_NAME"
echo "LOG $LOG"
echo "LATEST $LATEST_FILE"
echo "RUNNER $RUNNER"
