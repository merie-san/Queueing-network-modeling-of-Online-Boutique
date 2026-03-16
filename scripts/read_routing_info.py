import json
import sys

if __name__=="__main__":
    with open(sys.argv[1], "r") as f:
        data=json.load( f)

    n_interactions=data["call matrix"]
    call_n_row=data["tot callers"]

    for i in range(len(n_interactions)):
        print(i, end="\t")
        for j in range(len(n_interactions[i])):
            if call_n_row[i] != 0:
                print(f"{n_interactions[i][j] / call_n_row[i]:.5f}", end="\t")
            else:
                print("nan", end="\t")
        print()