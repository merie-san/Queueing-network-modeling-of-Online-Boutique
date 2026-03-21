import json
import os
import argparse

WK_DIR=os.environ.get("WKDIR")

def convert_to_json_metrics():
    metrics_paths = [
        path
        for path in os.listdir(WK_DIR)
        if path == "metrics.json"
        or (path.startswith("metrics") and path.endswith(".json"))
    ]
    metrics = []

    with open(f"{WK_DIR}/f_metrics.json", "w") as w:
        for metrics_path in metrics_paths:
            with open(f"{WK_DIR}/{metrics_path}", "r") as metrics_file:
                for line in metrics_file:
                    metrics.append(json.loads(line))
        json.dump(metrics, w, indent=4)


def convert_to_json_traces():
    traces_paths = [
        path
        for path in os.listdir(WK_DIR)
        if path == "traces.json"
        or (path.startswith("traces") and path.endswith(".json"))
    ]
    traces = []
    for traces_path in traces_paths:
        with open(f"{WK_DIR}/{traces_path}", "r") as traces_file:
            for line in traces_file:
                traces.append(json.loads(line))
    json.dump(traces, open(f"{WK_DIR}/f_traces.json", "w"), indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--metrics", action="store_true")
    parser.add_argument("-t", "--traces", action="store_true")
    args = parser.parse_args()
    if args.metrics:
        convert_to_json_metrics()
    if args.traces:
        convert_to_json_traces()
