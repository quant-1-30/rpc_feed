#! /bin/bash

# set +e # warning continue

CURRENT_DIR=$(pwd)
export PYTHONPATH=$CURRENT_DIR:$PYTHONPATH

# 检查 Poetry 是否安装
if ! command -v poetry &> /dev/null; then
    echo "Poetry is not installed. Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
fi

# 检查虚拟环境是否存在，如果不存在则创建
if [ ! -d "$(poetry env info --path 2>/dev/null)" ]; then
    echo "Creating Poetry virtual environment..."
    poetry install --no-root
fi

echo "Starting server in Poetry environment..."
poetry run python rpc_feed/run_server.py


# launchctl load ~/Library/LaunchAgents/com.example.graph-poetry.plist
# launchctl start com.example.graph-poetry
# launchctl unload ~/Library/LaunchAgents/com.example.graph-poetry.plist

# nohup python your_entry_script.py > out.log 2>&1 & # 0 stdin / 1 stdout / 2 stderr
