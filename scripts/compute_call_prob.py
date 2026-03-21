import json
from datetime import datetime
import math
import os

WK_DIR = os.getenv("WKDIR")

frontend_endpoints = [
    "GET /",
    "GET /product/{id}",
    "POST /cart/empty",
    "POST /setCurrency",
    "POST /cart/checkout",
    "GET /cart",
    "POST /cart",
    "GET /logout",
]
other_endpoints = [
    "GetAds",
    "ListRecommendations",
    "GetCart",
    "AddItem",
    "EmptyCart",
    "PlaceOrder",
    "Charge",
    "GetQuote",
    "ShipOrder",
    "SendOrderConfirmation",
    "GetSupportedCurrencies",
    "Convert",
    "ListProducts",
    "GetProduct",
]
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


def map_to_backend_endpoints(string) -> str | None:
    for other_endpoint in other_endpoints:
        if other_endpoint.lower() in string.lower():
            return other_endpoint
    return None


def map_to_frontend_endpoints(string) -> str | None:
    if string in frontend_endpoints:
        return string
    if string.split("/")[1] == "product":
        return "GET /product{id}"
    return None


endpoint_call_count_dict = {service: {} for service in services}

with open(f"{WK_DIR}/f_traces_filtered.json", "r") as f:
    traces_data_list = json.load(f)
    for trace_data in traces_data_list:
        for resourceSpan in trace_data["resourceSpans"]:
            resource_name = ""
            for attribute in resourceSpan["resource"]["attributes"]:
                if (
                    attribute["key"] == "service.name"
                    or attribute["key"] == "host.name"
                ):
                    for service in services:
                        if service in attribute["value"]["stringValue"].lower():
                            resource_name = service
                    break
            if resource_name == "":
                raise Exception(
                    "service.name and host.name attributes not found in resource attributes\n"
                    + str(resourceSpan["resource"]["attributes"])
                )

            for scopeSpan in resourceSpan["scopeSpans"]:
                for span in scopeSpan["spans"]:
                    span_kind = span["kind"]
                    unknown_client = True
                    unknown_server = True
                    backend_endpoint = None
                    url = None
                    method = None
                    for attribute in span["attributes"]:
                        if span_kind == 3:
                            if (
                                attribute["key"] == "rpc.method"
                                or attribute["key"] == "grpc.method"
                            ) and not backend_endpoint:
                                backend_endpoint = map_to_backend_endpoints(
                                    attribute["value"]["stringValue"]
                                )
                    if span_kind == 3:
                        if backend_endpoint:
                            if (
                                backend_endpoint
                                in endpoint_call_count_dict[resource_name]
                            ):
                                endpoint_call_count_dict[resource_name][
                                    backend_endpoint
                                ] += 1
                            else:
                                endpoint_call_count_dict[resource_name][
                                    backend_endpoint
                                ] = 1
                        elif url and method:
                            frontend_endpoint = map_to_frontend_endpoints(
                                method + " " + url
                            )
                            if (
                                frontend_endpoint
                                in endpoint_call_count_dict[resource_name]
                            ):
                                endpoint_call_count_dict[resource_name][
                                    frontend_endpoint
                                ] += 1
                            else:
                                endpoint_call_count_dict[resource_name][
                                    frontend_endpoint
                                ] = 1
                        else:
                            raise Exception(
                                "Span lack both backend endpoint and frontend endpoit: "
                                + str(span)
                            )

    service_call_count_dict = {
        service: {service: 0.0 for service in services} for service in services
    }
    for service, call_dict in endpoint_call_count_dict.items():
        for function_name, count in call_dict.items():
            service_call_count_dict[service][function_map[function_name]] += count
        for service_name in services:
            service_call_count_dict[service][service_name] = service_call_count_dict[
                service
            ][service_name]

    with open(f"{WK_DIR}/calls_count_{datetime.now().isoformat()}.json", "w") as f:
        json.dump(service_call_count_dict, f, indent=4)
