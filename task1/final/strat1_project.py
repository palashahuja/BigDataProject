from collections import Counter
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from itertools import chain
from pyspark.sql import SparkSession
import shutil
import sys
import json
import statistics
from dateutil.parser import parse
from datetime import datetime
import time
import os
import gc
import math

@f.udf
def calculate_statistics(x, column_name):
    output = Counter(x)
    top_five_count = [d[0] for d in output.most_common(5)]
    empty_cell_count = sum([value for key,value in output.items() if len(str(key)) == 0 or str(key).isspace() or str(key) == ''])
    # store the column data
    column_data = {}
    column_data['column_name'] = str(column_name)
    column_data['number_non_empty_cells'] = len(x) - empty_cell_count
    column_data['number_empty_cells'] = empty_cell_count
    column_data['frequent_values'] = top_five_count
    hs = {}
    for i in x:
        if i == None:
            continue
        try:
            y = float(i)
            # parsing nan or infinity as string
            if math.isnan(y) or y == float("inf") or y == float("-inf"):
                if 'string' in hs:
                    hs['string']['l'].append(i)
                    hs['string']['count'] += 1
                    hs['string']['sum'] += len(i)
                else:
                    hs['string'] = {'l':[i], 'sum' : len(i), 'count':1}
                continue
                
            try:
                z = int(i)
                if 'int' in hs:
                    hs['int']['l'].append(z)
                    hs['int']['count'] += 1
                    hs['int']['max'] = max(z,hs['int']['max'])
                    hs['int']['min'] = min(z,hs['int']['min'])
                    hs['int']['sum'] += z
                else:
                    hs['int'] = {'l' : [z], 'count' : 1, 'max':z, 'min':z, 'sum':z}
            except ValueError:
                if 'real' in hs:
                    hs['real']['l'].append(y)
                    hs['real']['count'] += 1
                    hs['real']['max'] = max(y,hs['real']['max'])
                    hs['real']['min'] = min(y,hs['real']['min'])
                    hs['real']['sum'] += y
                else:
                    hs['real'] = {'l' : [y], 'count' : 1, 'max':y, 'min':y, 'sum':y} 
            except:
                pass
        except:
            try:
                dt = parse(i)
                if 'dt' in hs:
                    hs['dt']['count'] += 1
                    if hs['dt']['max'][0] < dt:
                    	hs['dt']['max'] = [dt,i]
                    if hs['dt']['min'][0] > dt:
                        hs['dt']['min'] = [dt,i]
                else:
                    hs['dt'] = {'count' : 1, 'max' : [dt,i], 'min' : [dt,i]}
            except:
                if 'string' in hs:
                    hs['string']['l'].append(i)
                    hs['string']['count'] += 1
                    hs['string']['sum'] += len(i)
                else:
                    hs['string'] = {'l':[i], 'sum' : len(i), 'count':1}
    datas = []
    for key in hs:
        temp = {}
        if key == 'int' or key == 'real':
            if key == 'int':
                temp["type"] = "INTEGER (LONG)"
            else:
                temp["type"] = "REAL"
            temp["count"] = hs[key]['count']
            temp["max_value"] = round(hs[key]['max'],2)
            temp["min_value"] = round(hs[key]['min'],2)
            temp["mean"] = round(hs[key]['sum']/hs[key]['count'],2)
            temp["std_dev"] = round(statistics.stdev(hs[key]['l']), 2) if len(hs[key]['l']) > 1 else 0
        elif key == 'dt':
            temp["type"] = "DATE/TIME"
            temp["count"] = hs[key]["count"]
            temp["max_value"] = hs[key]["max"][1]
            temp["min_value"] = hs[key]["min"][1]
        else:
            temp["type"] = "TEXT"
            temp["count"] = hs[key]["count"]
            sortedd = sorted(hs[key]['l'])
            temp["shortest_value"] = sortedd[:5]
            temp["longest_value"] = sortedd[hs[key]['count'] - 5 :][::-1]
            temp["average_length"] = hs[key]["sum"]/hs[key]['count']
        datas.append(temp)
    column_data["data_types"] = datas
    return json.dumps(column_data)    

if len(sys.argv) < 2:
    print ("No Input File given")
    sys.exit(0)

file_name = sys.argv[1]
print(file_name)
ff = open("small.txt", 'r')
ll = ff.readlines()
ff.close()

out = open("files_succeeded.txt", 'a+')
#total_time = 0
spark = SparkSession.builder.appName("session_new").getOrCreate()
try:
    dataset = spark.read.options(header='true', inferschema='false', sep='\t').csv('/user/hm74/NYCOpenData/' + file_name + '.tsv.gz')
    start_time = time.time()
    top_five_count = [calculate_statistics(f.collect_list("'" + c + "'" ), f.lit(c)).alias(c) if '.' in c else calculate_statistics(f.collect_list(c), f.lit(c)).alias(c) for c in dataset.columns]
    output = dataset.agg(*top_five_count).collect()
    dict_output = []
    for col_val in dataset.columns:
        dict_output.append(json.loads(output[0].__getattr__(col_val)))
    final_output = {"dataset_name" :  file_name, "columns": dict_output}
    end_time = time.time()
    print (end_time - start_time)
    #total_time += (end_time - start_time)
    with open("jsons/" + file_name + ".json", 'w+') as out_file:
        json.dump(final_output, out_file, indent=4)
    out.write(file_name + '\n')
except Exception as e:
    print(e)
#gc.collect()
#spark.catalog.clearCache()

spark.stop()
out.close()
