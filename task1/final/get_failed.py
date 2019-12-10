f = open("small.txt", 'r')
files = f.readlines()
f.close()

for i in range(len(files)):
    files[i] = files[i].split(".")[0].strip() + ".json"

import os
x = "jsons/"
gg = os.listdir(x)
out = list(set(files).difference(set(gg)))
with open("failed.txt", 'w+') as bt:
    for i in out:
        bt.write(i.split(".")[0] + '\n')
