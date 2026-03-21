import json

if __name__ == "__main__":
    with open("../data/f_traces_filtered.json", "r") as f:
        traces_data_list = json.load(f)

        for traces_data in traces_data_list:
            for resourceSpan in traces_data.get("resourceSpans", []):
                for scopeSpan in resourceSpan.get("scopeSpans", []):
                    # keep only non-health spans
                    for span in scopeSpan.get("spans", []):
                        if "parentSpanId" not in span and span["name"] != "frontend" and span["kind"]==2:
                            raise Exception("Found a root span which is not product of a receiving call on the frontend: "+str(span))
        
        print("Check completed successfully.")