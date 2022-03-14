Using Airflow to orchestrate a data pipeline that loads two sets of JSON files (song data and log data) into a star schema in Redshift. We use staging tables as a placeholder and execute the whole pipeline via a DAG in Airflow
udac_example_dag.py is the starting point of the program and calls the necessary operators
All other python files are the operators themselves
The fully constructed DAG is shown below
