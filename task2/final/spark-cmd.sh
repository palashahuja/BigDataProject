mkdir -p column_output
spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/project_env/bin/python  --archives project_env.zip#env  $1 
python task2_join_jsons.py
