# Task 1

Once, the `project_env.zip` file is placed here, 
the following tasks need to be run

```
./test_run.sh
```

This will generate a jsons directory that will contain the jsons for all the 1900 datasets. To concatenate all the jsons into a single file,run the following command 
```
python join_jsons.py
```

## Information about scripts 
- small.txt - contains file names which are smaller than 15mb in size
- large.txt -  contains file names which are greater than 15mb in size
- strat1_project.py - For running on files which are lesser
than 15mb in size
- strat2_project.py - For running on files which are greater than 15mb in size