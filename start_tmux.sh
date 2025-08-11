#!/bin/bash

# 简单的 tmux 启动脚本
SESSION_NAME="filteringData"

case "$1" in
    start)
        echo "启动 filteringData..."
        if tmux has-session -t $SESSION_NAME 2>/dev/null; then
            tmux kill-session -t $SESSION_NAME
        fi
        tmux new-session -d -s $SESSION_NAME -n "main"
        tmux send-keys -t $SESSION_NAME:main "./filteringData" Enter
        echo "已启动，使用 'tmux attach -t filteringData' 连接"
        ;;
    stop)
        echo "停止 filteringData..."
        tmux kill-session -t $SESSION_NAME 2>/dev/null
        echo "已停止"
        ;;
    attach)
        echo "连接到会话..."
        tmux attach -t $SESSION_NAME
        ;;
    *)
        echo "使用方法: $0 [start|stop|attach]"
        echo "  start  - 启动程序"
        echo "  stop   - 停止程序"
        echo "  attach - 连接到会话"
        ;;
esac 