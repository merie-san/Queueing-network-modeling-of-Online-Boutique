import json
import os

WK_DIR = os.getenv("WKDIR")


def check_healthz_url_path(span):
    for attribute in span.get("attributes", []):
        if attribute["key"] == "url.path":
            return attribute["value"]["stringValue"] == "/_healthz"
    return False


def is_health_or_export_span(span):
    return (
        "health" in span["name"].lower()
        or check_healthz_url_path(span)
        or "export" in span["name"].lower()
    )


if __name__ == "__main__":
    with open(f"{WK_DIR}/f_traces.json") as f, open(
        f"{WK_DIR}/f_traces_filtered.json", "w"
    ) as f_out:
        traces_data_list = json.load(f)

        for traces_data in traces_data_list:
            for resourceSpan in traces_data.get("resourceSpans", []):
                for scopeSpan in resourceSpan.get("scopeSpans", []):
                    # keep only non-health spans
                    scopeSpan["spans"] = [
                        span
                        for span in scopeSpan.get("spans", [])
                        if not is_health_or_export_span(span)
                    ]

        json.dump(traces_data_list, f_out, indent=4)
