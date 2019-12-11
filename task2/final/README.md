# Task 2
For running task 2, one needs to run the following command 
```
./spark-cmd.sh
```
This will run the task 2 and create jsons for all the 264 labels in the folder `column_jsons`
To create a single file for all the predictions, you need to run the following command 
```
python task2_join_jsons.py
```
To get the json file that contains manual labels, one needs to run the following command 
```
python manual_jsons.py
```
For calculating precision and recall, one can run the following command 
```
python task2_stats.py
```

## Information about datasets 
The datasets_file contain all the datasets that are there for matching against constant values, and `all_train_data.json` contains training data for learning the multinomial naive bayes model.