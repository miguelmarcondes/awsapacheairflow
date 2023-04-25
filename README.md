# Project: Airflow Data Pipeline

Project developed using Python, Apache Airflow and AWS environment.

## Project Files
### dags/
- main_dag.py -- Initializes DAG, operators and tasks
- sql_queries -- SQL queries focused on creating tables and inserting data
### plugins/operators/
- \_\_init\_\_.py -- Initializes operator directory as python packages 
- created_table_check.py -- Creates needed tables if they don't exist
- stage_redshift.py -- Stages S3 data in Redshift
- load_dimensions.py -- Loads S3 data into dimension tables
- load_fact.py -- Loads S3 data into fact table
- data_quality.py -- Operator focused on checking if the data is following a set standard
