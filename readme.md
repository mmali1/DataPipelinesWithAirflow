# Project: Data pipelines with Airflow #

## Purpose ##

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The purpose of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Alon with these features we also need to ensure data quality. 

## Datasets ##

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
Here are the paths to two datasets,

* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Airflow tasks ##

We will create custom operators to perform tasks such as staging the data from s3 to redshift, loading facts and dimension tables, and perform data quality checks. Task dependecies are set to make sure the data pipeline has a coherent, easy to understand structure. 

## Dag configuration ##

In the DAG, `default parameters` are set according to these guidelines,

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

In addition to these task dendencies are set to reflect, final dag similar to following,

![Example_dag_flowchart](https://drive.google.com/file/d/1EOJvH5Ljr--GwLCeIIm-5oiphJFo7YyZ/view?usp=sharing)

## Operators ##

Four custom operators are built to be able to perform the tasks specified in image above.

* Stage to redshift operator: *
	The stage operator is expected to load any json formatted file from s3 to redshift. 
    The operator creates and runs a SQL COPY statement based on the parameters provided. 
    The operator's parameters should specify where in S3 the file is loaded and what is the target table.
    The parameters should be used to distinguish between JSON file.
    If needed, operator should be able to load timestamped data files
 
* Fact and dimension operators: *
	Fact and dimension operators make use of provided SQL helper class to run data transformations
    Operator takes input as sql statement and target table to run the query against
    When required in dimesion operator append_insert mode can be set to true, which will allow to load new data to dimension table in append mode as opposed to truncate then insert mode

* Data quality operator: *
	This operator performs the data quality checks against the tables loaded with fact and dimension operator
    The operator receives list of tables as an input, runs the sql query to check if there are any rows present or not
    In case of no rows, this operator will raise an exception and fail.
    
## How to run ##

* Run `opt/airflow/start.sh` to start the airflow server
* In the airflow server, create connections for AWS and redshift
* Turn the dag on in airflow server

