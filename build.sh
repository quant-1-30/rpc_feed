#! /bin/bash

# set +e # warning continue

# initialize database
poetry run python deploy_database.py

# activte web
cd rpc_feed && poetry run python rpc_svr.py
