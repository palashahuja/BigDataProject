# Big Data Project

## Team Members
- Palash Ahuja (pa1371)
- Sandeep Nandimandalam (sn2610)
- Surya Teja Sharma (sts419)

For running the project, we have used a custom anaconda environment, called `project-env`. This environment can be created as follows

```
conda create -n project_env --file requirements.txt
```
Then one can activate the `project_env` by running the following command 
```
conda activate project_env
```
Once, the anaconda environment is activated one also needs to make sure that the ```PYSPARK_PYTHON``` also needs to make the variable set to where python is installed . This is necessary for `pandas_udf` to work
```
export PYSPARK_PYTHON=<path-to-where-python-is-installed-for-environment>
```
Then one can run spark commands for all the projects.

For running task 1 and task2 ,  
`task1/final` - contains the final scripts for running task1  
`task2/final` - contains the final scripts for running task2


Before running the scripts, the file `project_env.zip` needs to be placed in both these directories, The file is located on dumbo at the 
following path `/home/pa1371/project_env.zip`. The file is also available at [this link](https://drive.google.com/open?id=1x3KvnNtGIQZhV-FH3BmezxOmtG89QW-l)



