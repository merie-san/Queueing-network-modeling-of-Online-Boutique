import json
import sys

def get_datapoints(metric):
    if "sum" in metric:
        return metric["sum"]["dataPoints"]
    elif "histogram" in metric:
        return metric["histogram"]["dataPoints"]
    else:
        raise Exception(f"sum and histogram keys are absent in metric {metric}")


if __name__ == "__main__":
    services = [
        "frontend",
        "adservice",
        "recommendationservice",
        "cartservice",
        "checkoutservice",
        "paymentservice",
        "shippingservice",
        "emailservice",
        "currencyservice",
        "productcatalogservice",
    ]
    service_metrics = {service: [] for service in services}
    with open("../data/f_metrics.json", "r") as f:
        json_metrics = json.load(f)
        for json_metric in json_metrics:
            for resourceMetric in json_metric["resourceMetrics"]:
                for scopeMetric in resourceMetric["scopeMetrics"]:
                    for metric in scopeMetric["metrics"]:
                        if any(
                            name_keyword in metric["name"]
                            for name_keyword in [
                                "requests_total",
                                "requests_duration",
                                "active_requests",
                            ]
                        ):
                            metric_name = metric["name"]
                            for datapoint in get_datapoints(metric):
                                function_name = None
                                endpoint_name = None
                                method_name = None
                                if "attributes" not in datapoint:
                                    continue
                                try:
                                    for attribute in datapoint["attributes"]:
                                        if attribute["key"] == "function":
                                            function_name = attribute["value"][
                                                "stringValue"
                                            ]
                                            break
                                        if attribute["key"] == "endpoint":
                                            endpoint_name = attribute["value"][
                                                "stringValue"
                                            ]
                                        if attribute["key"] == "method":
                                            method_name = attribute["value"]["stringValue"]
                                except KeyError:
                                    print(f"Encountered a keyError: {datapoint}")
                                    sys.exit()
                                if endpoint_name == "/_healthz":
                                    continue
                                if not function_name:
                                    if method_name and endpoint_name:
                                        function_name = (
                                            method_name + " " + endpoint_name
                                        )
                                    else:
                                        continue
                                for service in services:
                                    if metric_name.split("_")[0] in service:
                                        try:
                                            if "sum" in metric:
                                                service_metrics[service].append(
                                                    {
                                                        "resource_name": service,
                                                        "function_name": function_name,
                                                        "metric_name": metric_name,
                                                        "start_time": int(datapoint[
                                                            "startTimeUnixNano"
                                                        ])
                                                        / (10.0**9),
                                                        "time": int(datapoint["timeUnixNano"])
                                                        / (10.0**9),
                                                        "asInt": int(datapoint["asInt"]) if "asInt" in datapoint else int(datapoint["asDouble"]),
                                                    }
                                                )
                                            elif "histogram" in metric:
                                                service_metrics[service].append(
                                                    {
                                                        "resource_name": service,
                                                        "function_name": function_name,
                                                        "metric_name": metric_name,
                                                        "start_time": int(datapoint[
                                                            "startTimeUnixNano"
                                                        ])
                                                        / (10.0**9),
                                                        "time": int(datapoint["timeUnixNano"])
                                                        / (10.0**9),
                                                        "count": int(datapoint["count"]),
                                                        "sum": float(datapoint["sum"]),
                                                        "min": float(datapoint["min"]),
                                                        "max": float(datapoint["max"]),
                                                        "bucketCounts": datapoint[
                                                            "bucketCounts"
                                                        ],
                                                        "explicitBounds": datapoint[
                                                            "explicitBounds"
                                                        ],
                                                    }
                                                )
                                        except KeyError:
                                            print(f"Encountered a keyError: {datapoint}")
                                            sys.exit()

        json.dump(service_metrics, open("../data/f_processed_metrics.json", "w"), indent=4)
