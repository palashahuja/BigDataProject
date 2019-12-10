import json
import os

dirr = "jsons/"
out = {"datasets" : []}
for i in os.listdir(dirr):
    with open(dirr+i, "rb") as f:
        x = json.load(f)
    #print (x)
    out["datasets"].append(x)
with open("datasets.json", "w") as f:
    json.dump(out,f)
