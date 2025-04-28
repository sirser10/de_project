# MANATAL RAW DATA ETL
### Description
 The project consists of some components which ensure ETL-process of given data.
 Navigation:
 1) dags/manatal_project/manatal_recruitment_dag.py - Airflow object that ensures iterative orchestration of ETL-process
 2) operators: 
    - dags/manatal_project/file_loader_operator.py - Python script which helps us to create main logic of Extracting interview_data_with_anomalies.csv and Processing it.
    - dags/manatal_project/pg_operator.py - Custom Postgres Python Script which helps us to load the data into SRC. schema (initial layer) PostgreSQL DataBase (you can find it within docker-compose.yaml)
    - dags/manatal_project/sql_script_executor.py - Custom Python script which is aimed to automatically read and complete SQL-script
3) SQL-scripts:
    - dags/manatal_project/sql/manatal/stg/manual_interviewed_data.sql - SQL script that clean given data from initital SRC. layer
    - dags/manatal_project/sql/manatal/ods/manual_interviewed_data.sql - SQL script that UPSERT data by PRIMARY KEY CONSTRAINT (append every new data row from STG. layer)
    - dags/manatal_project/sql/manatal/dds/dim_candidate_addresses.sql - SQL script that creates (Slowly changed dimension) Table of candidate address  attributes with history
    - dags/manatal_project/sql/manatal/dds/dim_offer_details.sql       - SQL script that creates (Slowly changed dimension) Table of candidate offer attributes with history
    - dags/manatal_project/sql/manatal/dds/dim_personal_info.sql       - SQL script that creates (Slowly changed dimension) Table of candidate personal data with history

4) hook:
    - dags/manatal_project/hooks/pg_hook.py - Python script that helps Airflow to connect to PostgreS DB
5) helpers:
    - dags/manatal_project/helpers/common_helpers.py - Helps to get and put metadata columns to file

6) data_samples:
    - dags/manatal_project/data_samples/interview_data_with_anomalies.csv - given file of raw interview data
    - dags/manatal_project/data_samples/file_hash_logshran_file_hashes.txt - file with hash given file. It helps us to automatically check if any changes were in file to automatically run DAG
7) config:
    - dags/manatal_project/config/manual_table_config.py - Dictionary of propper renaming columns within given file and propper table name within PostgreS

### Airflow DAG

    



