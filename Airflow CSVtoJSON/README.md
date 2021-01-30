# Converting CSV File to JSON in AIRFLOW

Apache Airflow uses Python functions, as well as Bash or other operators, to create tasks that can be combined into a Directed Acyclic Graph (DAG) – meaning each task moves in one direction when completed. Airflow allows you to combine Python functions to create tasks. You can specify the order in which the tasks will run, and which tasks depend on others. This order and dependency are what make it a DAG. Then, you can schedule your DAG in Airflow to specify when, and how frequently, your DAG should run. Using the Airflow GUI, you can monitor and manage your DAG. 

Building a CSV to a JSON data pipeline

