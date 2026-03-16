import json
import os

traces_paths=[path for path in os.listdir("../data") if path == "traces.json" or (path.startswith("traces") and path.endswith(".json"))]
metrics_paths=[path for path in os.listdir("../data") if path=="metrics.json" or (path.startswith("metrics") and path.endswith(".json"))]
traces=[]
metrics=[]

with open("../data/f_metrics.json", "w") as w:
    for metrics_path in metrics_paths:
        with open(f"../data/{metrics_path}", "r") as metrics_file:
            for line in metrics_file:
                metrics.append(json.loads(line))
    json.dump(metrics, w, indent=4)

for traces_path in traces_paths:
    with open(f"../data/{traces_path}", "r") as traces_file:
        for line in traces_file:
            traces.append(json.loads(line))
json.dump(traces, open("../data/f_traces.json", "w"), indent=4)