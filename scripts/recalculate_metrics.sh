#! /usr/bin/bash
set -e pipefail
python3 convert_to_valid_json.py
python3 metrics_processing.py
python3 trace_filtering.py