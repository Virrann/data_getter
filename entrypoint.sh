#!/bin/bash

echo "🚀 Starting Jupyter..."

jupyter lab \
  --ip=0.0.0.0 \
  --port="${JUPYTHER_PORT:-8888}" \
  --no-browser \
  --allow-root \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.disable_check_xsrf=True &

echo "🖥️ Dropping into bash..."

exec bash