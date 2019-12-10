import json
import os

dirr = "column_output/"
out = {"predicted_types" : []}
for i in os.listdir(dirr):
    with open(dirr+i, "rb") as f:
        x = json.load(f)

    out["predicted_types"].append(x)
with open("task2.json", "w") as f:
    json.dump(out,f)