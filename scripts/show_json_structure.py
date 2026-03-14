import json
import os


with open("../data/traces.json", "r") as traces_file, open("../data/metrics.json", "r") as metrics_file:
    traces=[]
    metrics=[]
    if not os.path.exists("../data/traces_valid_format.json") or not os.path.exists("../data/metrics_valid_format.json"):
        for line in traces_file:
            traces.append(json.loads(line))
        for line in metrics_file:
            metrics.append(json.loads(line))
        json.dump(traces, open("../data/traces_valid_format.json", "w"), indent=4)
        json.dump(metrics, open("../data/metrics_valid_format.json", "w"), indent=4)
    else:
        traces = json.load(open("../data/traces_valid_format.json", "r"))
        metrics = json.load(open("../data/metrics_valid_format.json", "r"))

    # For browsing the json files using some json viewer
    json.dump(traces[:50], open("../data/trace_structure.json", "w"), indent=4)
    json.dump(metrics[:50], open("../data/metric_structure.json", "w"), indent=4)