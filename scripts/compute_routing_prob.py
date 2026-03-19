import json


class Span:

    def __init__(self, span) -> None:
        self.trace_id = span["traceId"]
        self.kind = span["kind"]
        self.start_time = int(span["startTimeUnixNano"]) / 10.0**9
        self.end_time = int(span["endTimeUnixNano"]) / 10.0**9
        self.name = span["name"].split("/")[-1]

    def get_start_time(self) -> float:
        return self.start_time

    def __repr__(self) -> str:
        return f"Span <name:{self.name},trace-id:{self.trace_id},kind:{self.kind},start-time:{self.start_time},end-time:{self.end_time}>"


function_map = {
    "GetAds": "adservice",
    "ListRecommendations": "recommendationservice",
    "GetCart": "cartservice",
    "AddItem": "cartservice",
    "EmptyCart": "cartservice",
    "PlaceOrder": "checkoutservice",
    "Charge": "paymentservice",
    "GetQuote": "shippingservice",
    "ShipOrder": "shippingservice",
    "SendOrderConfirmation": "emailservice",
    "GetSupportedCurrencies": "currencyservice",
    "Convert": "currencyservice",
    "ListProducts": "productcatalogservice",
    "GetProduct": "productcatalogservice",
}

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


def function_to_service(function_name) -> str:
    if function_name == "frontend":
        return "frontend"
    return function_map[function_name]


if __name__ == "__main__":

    with open("../data/f_traces_filtered.json") as f:
        trace_data_list = json.load(f)
    traces = {}
    for trace_data in trace_data_list:
        for resourceSpan in trace_data.get("resourceSpans", []):
            for scopeSpan in resourceSpan.get("scopeSpans", []):
                for span in scopeSpan.get("spans", []):
                    first_part = span["name"].split("/")[0]
                    if (
                        first_part == "grpc.hipstershop.CurrencyService"
                        or first_part == "grpc.hipstershop.PaymentService"
                    ):
                        continue
                    span_obj = Span(span)
                    if span_obj.trace_id not in traces:
                        traces[span_obj.trace_id] = [span_obj]
                    else:
                        traces[span_obj.trace_id].append(span_obj)

    for trace_id in traces.keys():
        traces[trace_id].sort(key=Span.get_start_time)

    routing_dict = {service: {} for service in services}

    emptied = []
    for trace_id, spans in traces.items():
        alert = False
        root_service_name = function_to_service(spans[0].name)
        end_service_name =function_to_service(spans[-1].name)
        if end_service_name not in ["frontend", "currencyservice", "adservice", "cartservice"]:
            emptied.append(trace_id)
            continue
        while root_service_name != "frontend":
            if (
                root_service_name != "paymentservice"
                and root_service_name != "currencyservice"
            ):
                alert = True

            traces[trace_id].pop(0)
            if len(traces[trace_id]) <= 0:
                emptied.append(trace_id)
                break
            root_service_name = function_to_service(traces[trace_id][0].name)

        if trace_id not in emptied and alert:
            raise Exception(
                "Trace contain spans other than paymentservice and currencyservice before frontend: "
                + str(spans)
            )

    for trace_id in emptied:
        traces.pop(trace_id)

    for trace_id, spans in traces.items():
        if spans[0].kind != 2:
            raise Exception(
                "Root span of the trace is not of kind 2: "
                + str(spans[0])
                + "\n"
                + str(spans)
            )
        for i in range(len(spans)):
            if i==0:
                if "in" not in routing_dict[spans[i].name]:
                    routing_dict[spans[i].name]["in"]=1
                else:
                    routing_dict[spans[i].name]["in"]+=1
            last_service=function_to_service(spans[i].name)
            if i==len(spans)-1:
                if "out" not in routing_dict[last_service]:
                    routing_dict[last_service]["out"]=1
                else:
                    routing_dict[last_service]["out"]+=1
            if spans[i].kind == 2:
                continue
            routing_service = function_to_service(spans[i - 1].name)
            if spans[i].name not in routing_dict[routing_service]:
                routing_dict[routing_service][spans[i].name] = 1
            else:
                routing_dict[routing_service][spans[i].name] += 1

    with open("../data/f_routing.json", "w") as f:
        json.dump(routing_dict, f, indent=4)
