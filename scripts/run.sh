#! /bin/bash

# set +e # warning continue

CURRENT_DIR=$(pwd)
export PYTHONPATH=$CURRENT_DIR:$PYTHONPATH

# supervisorctl can not automate create log
touch /var/log/rpc_feed.error.log
touch /var/log/rpc_feed.out.log
chmod 666 /var/log/rpc_feed*.log

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

echo "Starting Initialzie database in Poetry environment..."
poetry run python script/pg_init.py

echo "Starting server in Poetry environment..."
poetry run python rpc_feed/run_server.py
