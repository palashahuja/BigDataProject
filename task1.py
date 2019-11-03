from pyspark.sql import SparkSession
from pyspark.sql.functions import col 


import sys
import os
import shutil
import json


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Task 1 SQL").getOrCreate()
    file_name = sys.argv[1]
    dataset = spark.read.options(header='true', \
            inferschema='true', sep='\t').csv(file_name)
    # get the dataset attributes 
    dataset_name = file_name.split('/')[4].split('.')[0]
    # keep the dataset name 
    json_data = {}
    # store the dataset name
    json_data["dataset_name"] = dataset_name
    # store columns 
    json_data["columns"] = []
    # declare the functions 
    # iterate through all the columns 
    for column in dataset.columns:
        # number of non empty cells 
        non_empty_cell_count = dataset.filter(dataset[column].isNotNull()).count()
        # number of empty cells
        empty_cell_count = dataset.filter(dataset[column].isNull()).count()
        # get distinct values
        distinct_values = dataset.select(column).distinct().count()
        # top 5 frequent values 
        frequent_value_counts = dataset.groupBy(column).count().sort(col("count").desc()).take(5)
        # column json data 
        column_json_data = {}
        column_json_data["column_name"] = column
        column_json_data["number_non_empty_cells"] = non_empty_cell_count
        column_json_data["number_empty_cells"] = empty_cell_count
        column_json_data["number_distinct_values"] = distinct_values
        column_json_data["frequent_values"] = list(map(lambda x : x[column], frequent_value_counts))
        json_data["columns"].append(column_json_data)
    # dump the json data
    with open(dataset_name + '.json', 'w') as f:
        json.dump(json_data, f)

       



