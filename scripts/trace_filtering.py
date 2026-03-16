import json

def check_healthz_url_path(span):
    for attribute in span.get("attributes", []):
        if attribute["key"] == "url.path":
            return attribute["value"]["stringValue"] == "/_healthz"
    return False


def is_health_span(span):
    return "health" in span["name"].lower() or check_healthz_url_path(span)


if __name__ == "__main__":
    with open("../data/f_traces.json") as f, open("../data/f_traces_filtered.json", "w") as f_out:
        traces = json.load(f)

        for trace in traces:
            for resourceSpan in trace.get("resourceSpans", []):
                for scopeSpan in resourceSpan.get("scopeSpans", []):
                    # keep only non-health spans
                    scopeSpan["spans"] = [
                        span for span in scopeSpan.get("spans", [])
                        if not is_health_span(span)
                    ]

        json.dump(traces, f_out, indent=4)