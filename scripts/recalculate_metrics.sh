#! /usr/bin/bash
#export WKDIR=../data
set -e pipefail
python3 convert_to_valid_json.py -m
python3 metrics_processing.py
python3 compute_avg_metrics.py -a