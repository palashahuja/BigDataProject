import subprocess

f = open('op_small.log', 'w+')
p = subprocess.Popen(['./test.sh'], shell=True,stdout=f, stderr=f)
