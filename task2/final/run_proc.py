import subprocess

f = open('op_large.log', 'w+')
p = subprocess.Popen(['./spark-cmd.sh task2.py'], shell=True,stdout=f, stderr=f)
