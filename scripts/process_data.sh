#! /usr/bin/bash
set -e pipefail
python3 show_json_structure.py
python3 metrics_processing.py
python3 trace_filtering.py