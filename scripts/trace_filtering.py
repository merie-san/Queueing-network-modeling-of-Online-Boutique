import json

def check_healthz_url_path(span):

    for attribute in span.get("attributes", []):
        if attribute["key"]=="url.path":
            return attribute["value"]["stringValue"]=="/_healthz"
    return False

if __name__=="__main__":
    with open("../data/traces_valid_format.json") as f, open("../data/traces_filtered.json", "w") as f_out:
        traces = json.load(f)
        filtered_traces = []
        for trace in traces:
            skip=False
            for resourceSpan in trace["resourceSpans"]:
                if skip:
                    break
                for scopeSpan in resourceSpan["scopeSpans"]:
                    if skip:
                        break
                    for span in scopeSpan["spans"]:
                        if "health" in span["name"].lower() or  "export" in span["name"].lower() or check_healthz_url_path(span):
                            skip=True
                            break
            if not skip:
                filtered_traces.append(trace)
        json.dump(filtered_traces, f_out, indent=4)

