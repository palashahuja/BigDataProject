import pyspark
import pandas as pd
from pyspark.sql.functions import pandas_udf, col, PandasUDFType
from pyspark.sql.types import StringType, IntegerType, LongType, DateType, DoubleType
from concurrent.futures.thread import ThreadPoolExecutor
import time
from pyspark.sql import SparkSession
from dateutil.parser import parse
import pyspark.sql.functions as f
import sys
import math
import json

input_file_name = sys.argv[1]
file_open = open(input_file_name, 'r')

def get_data_type(x):
    if x is None or str(x).isspace() or x == '' or len(str(x)) == 0:
        return 0
    try:
        y = float(x)
        if math.isnan(y) or y == float("inf") or y == float("-inf"):
            return 4
        try:
            z = int(x)
            return 1
        except:
            return 2
    except:
        try:
            w = parse(x)
            return 3
        except:
            return 4

# pandas udf function
@pandas_udf(IntegerType())
def data_type(x):
    #return x.apply(lambda t : get_data_type(t))
    op_types = {t : get_data_type(t) for t in x.unique()}
    return x.map(op_types)

@pandas_udf(StringType())
def get_string_format(inp):
    dates = {x : parse(x).strftime('%Y-%m-%d %H:%M:%S') for x in inp.unique()}
    return inp.map(dates)

@pandas_udf(IntegerType())
def return_length(inp1):
    return inp1.apply(lambda x : len(x))

all_column_jsons = {}
dataset_length = 0

def get_column_data(column):
    # intialize the json data for the column
    print(column)
    column_json_data = {}
    column_json_data["column_name"] = inv_mapping_dict[column]
    distn_vals = dataset.select(column).groupBy(column).count().orderBy('count', ascending=False).cache()
    column_json_data['frequent_values']        = list(map(lambda x : x.__getattr__(column), distn_vals.head(5)))
    column_json_data['number_distinct_values'] = distn_vals.count()
    start_time = time.time()
    output_data = dataset.select(data_type(column).alias('type'), col(column).alias('value'),
                                    f.round(f.rand()*1e10 + 2e10, 0).alias('random_value'))
    output_data = output_data.repartition(col('random_value')).cache()
    # get empty cell count
    empty_cell_count = output_data.filter(col('type') == '0').count()
    column_json_data["number_empty_cells"]     = empty_cell_count
    column_json_data["number_non_empty_cells"] = dataset_length - empty_cell_count
    column_json_data["data_types"] = []
    # get min, max, mean and standard_deviation for integer as well as real
    int_filt  = output_data.filter(col('type') == 1)
    int_count = int_filt.count()
    if int_count > 0:
        integer_report = int_filt.select(f.mean(col('value')).alias('mean'),
                            f.max(col('value').cast(IntegerType())).alias('max'),
                            f.min(col('value').cast(IntegerType())).alias('min'),
                            f.stddev(col('value')).alias('std')
                            ).collect()
        integer_json = {
            "type"      : "INTEGER (LONG)",
            "count"     : int_count,
            "max_value" : integer_report[0].__getattr__('max'),
            "min_value" : integer_report[0].__getattr__('min'),
            "mean"      : integer_report[0].__getattr__('mean'),
            "stddev"    : integer_report[0].__getattr__('std')
        }
        column_json_data["data_types"].append(integer_json)

    # filtering the floating values
    float_filt  = output_data.filter(col('type') == 2)
    float_count = float_filt.count()
    if float_count > 0:
        float_report = float_filt.select(f.mean(col('value')).alias('mean'),
                            f.max(col('value').cast(DoubleType())).alias('max'),
                            f.min(col('value').cast(DoubleType())).alias('min'),
                            f.stddev(col('value')).alias('std')
                            ).collect()
        float_json = {
            "type"      : "REAL",
            "count"     : float_count,
            "max_value" : float_report[0].__getattr__('max'),
            "min_value" : float_report[0].__getattr__('min'),
            "mean"      : float_report[0].__getattr__('mean'),
            "stddev"    : float_report[0].__getattr__('std')
        }
        column_json_data["data_types"].append(float_json)
    date_filt  = output_data.filter(col("type") == 3)
    date_count = date_filt.count()
    if date_count > 0:
        date_report = date_filt.select(get_string_format('value').alias('value'))\
                        .select(f.max(col('value')).alias('max'), 
                                f.min(col('value')).alias('min')).collect()
        date_json = {
            "type"       : "DATE/TIME",
            "count"      :  date_count, 
            "max_value"  :  date_report[0].__getattr__('max'),
            "min_value"  :  date_report[0].__getattr__('min')
        }
        column_json_data["data_types"].append(date_json)

    string_filt  = output_data.filter(col("type") == 4)
    string_count = string_filt.count()
    if string_count > 0:
        string_fil_le = string_filt.select('value', return_length('value').alias('length'))
        string_report = string_fil_le.select(f.avg(col('length')).alias('avg')).collect()
        string_fil_to = string_fil_le.orderBy(col('length'), ascending=True).head(5)
        string_fil_ba = string_fil_le.orderBy(col('length'), ascending=False).head(5)
        string_op1    = list(map(lambda x : x.__getattr__('value'), string_fil_to))
        string_op2    = list(map(lambda x : x.__getattr__('value'), string_fil_ba))
        string_json   = {
                "type"               : "TEXT",
                "count"              :  string_count,
                "max_value"          :  string_op2,
                "min_value"          :  string_op1,
                "average_length"     :  string_report[0].__getattr__('avg')
        }
        column_json_data["data_types"].append(string_json)
    all_column_jsons[column] = column_json_data


session_name = sys.argv[2]
spark = SparkSession.builder.appName(session_name).getOrCreate()

for line in file_open:
    line = line.strip()
    file_name = line
    print(file_name)
    dataset = spark.read.format('csv').options(header='true',inferschema='false',multiLine='true', delimiter='\t', encoding = "ISO-8859-1").load('/user/hm74/NYCOpenData/' + file_name + '.tsv.gz')
    mapping_dict = {}
    inv_mapping_dict = {}

    for index, column in enumerate(dataset.columns):
        mapping_dict[column] = 'column_' + str(index)
        inv_mapping_dict['column_' + str(index)] = column
    dataset = dataset.select([f.col("`" + column + "`").alias(mapping_dict[column]) for column in dataset.columns])
    dataset.write.mode('overwrite').parquet('/tmp/' + file_name + '.parquet')
    dataset = spark.read.parquet('/tmp/' + file_name + '.parquet')
    print('dataset loaded')
    dataset_length = dataset.count()
    all_column_jsons = {}
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=16) as executor:
         for column in dataset.columns:
             executor.submit(get_column_data, column)
    end_time = time.time()
    print('time taken is %s', str(end_time - start_time))
    print('writing to file')
    with open('jsons/' + file_name + '.json', 'w+') as new_file:
        all_column_json_list = []
        for key in all_column_jsons:
            all_column_json_list.append(all_column_jsons[key])
        output_json_file = {"dataset_name": file_name, "columns": all_column_json_list}
        new_file.write(json.dumps(output_json_file, indent=4))
    spark.catalog.clearCache()

