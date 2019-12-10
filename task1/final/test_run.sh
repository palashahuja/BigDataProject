#!/bin/bash

mkdir -p jsons

input="small.txt"
x=0
while IFS= read -r line1
do
    timeout 120 spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python  strat1_project.py "$line1"
    ((x+=1))
    echo "${line1}"
    echo "tasks done - $x"
    echo "/*------------------------------------------------------------------------------------------------*/"
done < $input

date

python get_failed.py

spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/project_env/bin/python  --archives project_env.zip#env --conf spark.yarn.executor.memoryOverhead=4096 --executor-memory 20G --conf spark.driver.memory=15g --executor-cores 5 --driver-cores 5 strat2_project.py large.txt session1
spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/project_env/bin/python  --archives project_env.zip#env --conf spark.yarn.executor.memoryOverhead=4096 --executor-memory 20G --conf spark.driver.memory=15g --executor-cores 5 --driver-cores 5 strat2_project.py failed.txt session1

