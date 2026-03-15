import json
import math


class RequestsCountCumulator:

    def __init__(self, metric) -> None:
        record_time, count = self._extract_metrics(metric)
        self.resource = metric["resource_name"]
        self.endpoint = metric["function_name"]
        self.record_time_1 = record_time
        self.record_time_2 = record_time
        self.count_1 = count
        self.count_2 = count

    def _extract_metrics(self, metric) -> tuple[float, int]:

        if any(
            key not in metric
            for key in ["resource_name", "metric_name","function_name", "time", "asInt"]
        ) or "requests_total" not in metric["metric_name"]:
            raise Exception("Metric provided is not in the expected format", self)

        if hasattr(self, "resource") and self.resource != metric["resource_name"]:
            raise Exception("Metric with wrong resource", self)

        if hasattr(self, "endpoint") and self.endpoint != metric["function_name"]:
            raise Exception("Metric with wrong endpoint", self)

        record_time = float(metric["time"])
        count = int(metric["asInt"])
        if record_time <= 0:
            raise Exception("Invalid time value", self)
        if count < 0:
            raise Exception("Invalid count value", self)
        if hasattr(self, "record_time_2") and hasattr(self, "count_2"):
            if record_time < self.record_time_2:
                raise Exception("time value smaller than previous update time", self)
            if count < self.count_2:
                raise Exception("count value smaller than previous count value", self)
        return record_time, count

    def update(self, metric):
        self.record_time_2, self.count_2 = self._extract_metrics(metric)

    def get_metric(self) -> float:
        if self.record_time_2 == self.record_time_1:
            raise Exception(
                "Average arrival rate cannot be computed from single datapoint"
            )
        return (self.count_2 - self.count_1) / (self.record_time_2 - self.record_time_1)

    def get_interval_length(self) -> float:
        return self.record_time_2 - self.record_time_1

    def reset(self):
        self.record_time_1 = self.record_time_2
        self.count_1 = self.count_2

    def __repr__(self) -> str:
        return f"\nRequestsCountCumulator:\nresource: {self.resource}\nendpoint: {self.endpoint}\nfirst datapoint: time - {self.record_time_1}, count - {self.count_1}\nsecond datapoint: time - {self.record_time_2}, count - {self.count_2}"


class RequestsDurationCumulator:

    def __init__(self, metric) -> None:
        record_time, count, sum = self._extract_metrics(metric)
        self.resource = metric["resource_name"]
        self.endpoint = metric["function_name"]
        self.record_time_1 = record_time
        self.count_1 = count
        self.sum_1 = sum
        self.record_time_2 = record_time
        self.count_2 = count
        self.sum_2 = sum

    def _extract_metrics(self, metric) -> tuple[float, int, float]:

        if any(
            key not in metric
            for key in ["resource_name", "metric_name", "function_name", "time", "count", "sum"]
        ) or "requests_duration" not in metric["metric_name"]:
            raise Exception("Metric provided is not in the expected format", self)

        if hasattr(self, "resource") and self.resource != metric["resource_name"]:
            raise Exception("Metric with wrong resource", self)

        if hasattr(self, "endpoint") and self.endpoint != metric["function_name"]:
            raise Exception("Metric with wrong endpoint", self)

        record_time = float(metric["time"])
        count = int(metric["count"])
        sum = float(metric["sum"])
        if record_time <= 0:
            raise Exception("Invalid time value", self)
        if count < 0:
            raise Exception("Invalid count value", self)
        if sum < 0:
            raise Exception("Invalid sum value", self)
        if (
            hasattr(self, "record_time_2")
            and hasattr(self, "count_2")
            and hasattr(self, "sum_2")
        ):
            if record_time < self.record_time_2:
                raise Exception("time value smaller than previous update time", self)
            if count < self.count_2:
                raise Exception("count value smaller than previous count value", self)
            if sum < self.sum_2:
                raise Exception("sum value smaller than previous sum value", self)
        return record_time, count, sum

    def update(self, metric):
        self.record_time_2, self.count_2, self.sum_2 = self._extract_metrics(metric)

    def get_metric(self) -> float:
        if self.count_2 == self.count_1:
            return math.nan
        return (self.sum_2 - self.sum_1) / (self.count_2 - self.count_1)

    def get_interval_length(self) -> float:
        return self.record_time_2 - self.record_time_1

    def reset(self):
        self.record_time_1 = self.record_time_2
        self.count_1 = self.count_2
        self.sum_1 = self.sum_2

    def __repr_(self) -> str:
        return f"\nRequestsDurationCumulator:\nresource: {self.resource}\nendpoint: {self.endpoint}\nfirst datapoint: time - {self.record_time_1}, count - {self.count_1}, sum - {self.sum_1}\nsecond datapoint: time - {self.record_time_2}, count - {self.count_2}, sum - {self.sum_2}"


class ActiveRequestsCumulator:

    def __init__(self, metric) -> None:
        record_time, count = self._extract_metrics(metric)
        self.resource = metric["resource_name"]
        self.endpoint = metric["function_name"]
        self.interval_start = record_time
        self.record_time_1 = record_time
        self.record_time_2 = record_time
        self.count_2 = count
        self.mean = count

    def _extract_metrics(self, metric) -> tuple[float, int]:

        if any(
            key not in metric
            for key in ["resource_name", "metric_name", "function_name", "time", "asInt"]
        ) or "active_requests" not in metric["metric_name"]:
            raise Exception("Metric provided is not in the expected format", self)

        if hasattr(self, "resource") and self.resource != metric["resource_name"]:
            raise Exception("Metric with wrong resource", self)

        if hasattr(self, "endpoint") and self.endpoint != metric["function_name"]:
            raise Exception("Metric with wrong endpoint", self)

        record_time = float(metric["time"])
        count = int(metric["asInt"])
        if record_time <= 0:
            raise Exception("Invalid time value", self)
        if count < 0:
            raise Exception("Invalid count value", self)
        if hasattr(self, "record_time_2"):
            if record_time < self.record_time_2:
                raise Exception("time value smaller than previous update time", self)
        return record_time, count

    def update(self, metric):
        record_time, count = self._extract_metrics(metric)
        if self.record_time_1 == record_time:
            self.record_time_2 = record_time
            self.count_2 = count
            self.mean = count
        else:
            self.mean = (
                self.mean * (self.record_time_2 - self.record_time_1)
                + 0.5 * (self.count_2 + count) * (record_time - self.record_time_2)
            ) / (record_time - self.record_time_1)
            self.record_time_1 = self.record_time_2
            self.record_time_2 = record_time
            self.count_2 = count

    def get_metric(self) -> float:
        return self.mean

    def get_interval_length(self) -> float:
        return self.record_time_2 - self.interval_start

    def reset(self):
        self.interval_start = self.record_time_2
        self.record_time_1 = self.record_time_2
        self.mean = self.count_2

    def __repr__(self) -> str:
        return f"\nActiveRequestsCumulator:\nresource: {self.resource}\nendpoint: {self.endpoint}\ninterval start: {self.interval_start}\nfirst datapoint: time - {self.record_time_1}\nsecond datapoint: time - {self.record_time_2}, count - {self.count_2}\nmean: {self.mean}"
    
    


if __name__ == "__main__":
    raw_metrics = json.load(open("../data/processed_metrics.json", "r"))
    # computing average throughput, average response time and average number of users for each endpoint in each microservice.
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
    metrics_dict={}
    for service in services:
        metrics_dict[service]={}
        endpoint_cumulator = {}
        for service_metric in raw_metrics[service]:
            function_name = service_metric["function_name"]
            metric_type="_".join(service_metric["metric_name"].split("_")[1:])
            if function_name not in endpoint_cumulator:
                endpoint_cumulator[function_name] = {"requests_total": None, "requests_duration": None, "active_requests": None}
                if metric_type == "requests_total":
                    endpoint_cumulator[function_name][metric_type]= RequestsCountCumulator(service_metric)
                    metrics_dict[service][function_name]={metric_type:[]}
                elif metric_type == "requests_duration":
                    endpoint_cumulator[function_name][metric_type]= RequestsDurationCumulator(service_metric)
                    metrics_dict[service][function_name]={metric_type:[]}
                elif metric_type == "active_requests":
                    endpoint_cumulator[function_name][metric_type]= ActiveRequestsCumulator(service_metric)
                    metrics_dict[service][function_name]={metric_type:[]}
                else:
                    raise Exception("Unexpected metric type")
            else:
                if not endpoint_cumulator[function_name][metric_type]:
                    if metric_type == "requests_total":
                        endpoint_cumulator[function_name][metric_type]= RequestsCountCumulator(service_metric)
                        metrics_dict[service][function_name][metric_type]=[]
                    elif metric_type == "requests_duration":
                        endpoint_cumulator[function_name][metric_type]= RequestsDurationCumulator(service_metric)
                        metrics_dict[service][function_name][metric_type]=[]
                    elif metric_type == "active_requests":
                        endpoint_cumulator[function_name][metric_type]= ActiveRequestsCumulator(service_metric)
                        metrics_dict[service][function_name][metric_type]=[]
                    else:
                        raise Exception("Unexpected metric type")
                else:
                    cumulator=endpoint_cumulator[function_name][metric_type]
                    cumulator.update(service_metric)
                    interval=cumulator.get_interval_length()
                    if interval>30:
                        metrics_dict[service][function_name][metric_type].append((cumulator.get_metric(),  interval))
                        cumulator.reset()

    for svc, svc_dict in metrics_dict.items():
        print(svc)
        for func_name, func_dict in svc_dict.items():
            print(func_name)
            for metric_name, metric_list in func_dict.items():
                print(f"average {metric_name} in subsequent intevals:")
                i=0
                for tuple_v in metric_list:
                    print(f"interval {i} of {tuple_v[1]} s - average metric {tuple_v[0]}")

                
    
    json.dump(metrics_dict,open("../data/average_metrics.json","w"), indent=4)
        


