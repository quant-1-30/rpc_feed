#! /bin/bash

# set +e # warning continue

# initialize database
poetry run python deploy_db.py
# activte web
cd rpc_feed && poetry run python rpc_svr.py

# launchctl load ~/Library/LaunchAgents/com.example.graph-poetry.plist
# launchctl start com.example.graph-poetry
# launchctl unload ~/Library/LaunchAgents/com.example.graph-poetry.plist

nohup python your_entry_script.py > out.log 2>&1 & # 0 stdin / 1 stdout / 2 stderr
