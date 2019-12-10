import subprocess

f = open('op_large.log', 'w+')
p = subprocess.Popen(['./test_run.sh'], shell=True,stdout=f, stderr=f)
