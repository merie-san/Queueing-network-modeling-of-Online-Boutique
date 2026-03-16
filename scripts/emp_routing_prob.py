import json
from datetime import datetime

with open("../data/f_traces_filtered.json", "r") as f, open(
    "service_cluster_ip.json", "r"
) as service_cluster_ip_f, open("pod_ip.json", "r") as pod_ip_f:
    traces = json.load(f)
    service_cluster_ip_mapping = json.load(service_cluster_ip_f)
    pod_ip_mapping = json.load(pod_ip_f)
    service_index_map = {
        "frontend": 0,
        "adservice": 1,
        "recommendationservice": 2,
        "cartservice": 4,
        "checkoutservice": 8,
        "paymentservice": 7,
        "shippingservice": 5,
        "emailservice": 9,
        "currencyservice": 6,
        "productcatalogservice": 3,
    }
    n_interactions = [[0 for _ in range(10)] for _ in range(10)]
    n_unknown_callers = [0 for _ in range(10)]
    n_unknown_callees = [0 for _ in range(10)]
    for trace in traces:
        for resourceSpan in trace["resourceSpans"]:
            resource_name = ""
            for attribute in resourceSpan["resource"]["attributes"]:
                if attribute["key"] == "service.name":
                    resource_name = attribute["value"]["stringValue"]
                    break
            if resource_name == "":
                raise Exception(
                    "service.name attribute not found in resource attributes\n"
                    + str(resourceSpan["resource"]["attributes"])
                )

            for scopeSpan in resourceSpan["scopeSpans"]:
                for span in scopeSpan["spans"]:
                    span_kind = span["kind"]
                    unknown_client = True
                    unknown_server = True
                    for attribute in span["attributes"]:
                        if span_kind == 2:
                            if (
                                attribute["key"] == "network.peer.address"
                                or attribute["key"] == "client.address"
                                or attribute["key"] == "net.peer.ip"
                            ):
                                client_address = attribute["value"]["stringValue"]
                                if client_address in service_cluster_ip_mapping:
                                    # If we get a service cluster ip we convert it to a service name
                                    client_address = service_cluster_ip_mapping[
                                        client_address
                                    ]
                                if client_address in pod_ip_mapping:
                                    # Same if we get pod ip
                                    client_address = pod_ip_mapping[
                                        client_address
                                    ].split("-")[0]
                                if client_address in service_index_map:
                                    # When we get a name that matches with a system service
                                    client_index = service_index_map[client_address]
                                    n_interactions[client_index][
                                        service_index_map[resource_name]
                                    ] += 1
                                    unknown_client = False
                                    break
                                else:
                                    # When we get a name that does not match any system services
                                    unknown_client = False
                                    break

                        elif span_kind == 3:
                            if attribute["key"] == "server.address":
                                server_address = attribute["value"]["stringValue"]
                                if server_address in service_cluster_ip_mapping:
                                    # If we get an ip address we first convert it to a service name
                                    server_address = service_cluster_ip_mapping[
                                        server_address
                                    ]
                                if server_address in pod_ip_mapping:
                                    # Same if we get pod ip
                                    server_address = pod_ip_mapping[
                                        server_address
                                    ].split("-")[0]
                                if server_address in service_index_map:
                                    # When we get a name that matches with a system service
                                    server_index = service_index_map[server_address]
                                    n_interactions[service_index_map[resource_name]][
                                        server_index
                                    ] += 1
                                    unknown_server = False
                                    break
                            elif attribute["key"] == "rpc.service":
                                # When we cannot get the service address directly we rely on the rpc name
                                service_name = attribute["value"]["stringValue"]
                                for service in service_index_map.keys():
                                    if service in service_name.lower():
                                        n_interactions[
                                            service_index_map[resource_name]
                                        ][service_index_map[service]] += 1
                                        unknown_server = False
                                        break

                    if unknown_client and span_kind == 2:
                        n_unknown_callers[service_index_map[resource_name]] += 1
                    if unknown_server and span_kind == 3:
                        n_unknown_callees[service_index_map[resource_name]] += 1

    print("Interaction matrix (rows are callers, columns are callees):")

    for key, value in service_index_map.items():
        print(f"{key} - {value}")

    print()
    print("\t", end="")
    for i in range(10):
        print(f"{i}", end="\t")
    print()
    for i in range(len(n_interactions)):
        print(i, end="\t")
        for j in range(len(n_interactions[i])):
            print(n_interactions[i][j], end="\t")
        print()

    print("Routing probability matrix.")
    call_n_row = []
    for i in range(len(n_interactions)):
        n = 0
        for j in range(len(n_interactions[i])):
            n += n_interactions[i][j]
        call_n_row.append(n)

    with open(f"../data/routing_info_{datetime.now().isoformat()}.json", "w") as f:
        json.dump({"tot caller": call_n_row, "call matrix": n_interactions}, f, indent=4)

    for i in range(len(n_interactions)):
        print(i, end="\t")
        for j in range(len(n_interactions[i])):
            if call_n_row[i] != 0:
                print(f"{n_interactions[i][j] / call_n_row[i]:.5f}", end="\t")
            else:
                print("nan", end="\t")
        print()

    print("Unknown callers per service:")
    for i in range(10):
        print(f"{i}", end="\t")
    print()
    for i in range(len(n_unknown_callers)):
        print(f"{n_unknown_callers[i]}", end="\t")
    print()
    print("Unknown callees per service:")
    for i in range(10):
        print(f"{i}", end="\t")
    print()
    for i in range(len(n_unknown_callees)):
        print(f"{n_unknown_callees[i]}", end="\t")
    print()
