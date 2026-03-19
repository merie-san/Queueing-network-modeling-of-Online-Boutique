#! /usr/bin/bash
export WKDIR=../data2
set -e pipefail
python3 convert_to_valid_json.py -m
python3 metrics_processing.py
python3 emp_avg_metrics.py -a