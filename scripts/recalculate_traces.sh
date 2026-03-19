#! /usr/bin/bash
set -e pipefail
python3 convert_to_valid_json.py -t
python3 trace_filtering.py
python3 compute_routing_prob.py