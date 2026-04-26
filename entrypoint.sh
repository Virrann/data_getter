#!/bin/bash

echo "🚀 Starting Jupyter..."

jupyter nbclassic \
  --ip=0.0.0.0 \
  --port="${JUPYTHER_PORT:-8888}"\
  --no-browser \
  --allow-root \
  --NotebookApp.token='' &

echo "🖥️ Dropping into bash..."

exec bash