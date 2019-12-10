import json
import os
import sys

flag = sys.argv[1]

if flag == 1:
    dirr = "jsons/"
    out = {"datasets" : []}
    for i in os.listdir(dirr):
        with open(dirr+i, "rb") as f:
            x = json.load(f)

        out["datasets"].append(x)
    with open("task1.json", "w") as f:
        json.dump(out,f)

if flag == 2:
    dirr = "task2_jsons/"
    out = {"predicted_types" : []}
    for i in os.listdir(dirr)
        with open(dirr+i, "rb") as f:
            x = json.load(f)
        out["predicted_types"].append(x)
    with open("task2.json", "w") as f:
        json.dump(out,f)
