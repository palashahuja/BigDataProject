import subprocess

f = open('op_large.log', 'w+')
p = subprocess.Popen(['./spark-cmd.sh op_file.py output.txt session1'], shell=True,stdout=f, stderr=f)
