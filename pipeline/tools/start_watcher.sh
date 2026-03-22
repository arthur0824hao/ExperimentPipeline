#!/bin/bash
# start_watcher.sh - 啟動 Auto-Wake Watcher

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SESSION="oc-experimentpipeline-watcher"

tmux has-session -t "$SESSION" 2>/dev/null
if [ $? != 0 ]; then
    echo "Creating watcher session..."
    tmux new-session -d -s "$SESSION" -c "$ROOT_DIR"
    tmux send-keys -t "$SESSION" "conda activate gnn_fraud" C-m
fi

tmux send-keys -t "$SESSION" "python pipeline/tools/watcher.py $@" C-m

echo "Watcher started in tmux session: $SESSION"
echo "Attach with: tmux attach -t $SESSION"
