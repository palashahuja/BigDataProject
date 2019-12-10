import json
import os
import sys


dirr = "jsons/"
out = {"datasets" : []}
for i in os.listdir(dirr):
    with open(dirr+i, "rb") as f:
        x = json.load(f)

    out["datasets"].append(x)
with open("task1.json", "w") as f:
    json.dump(out,f,indent=4)
