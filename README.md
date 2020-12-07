## S3 Files Load to MySQL Tables using Airflow:

## Prerequisite

1.  MySQL Database.
2.  S3 Bucket
3.  Airflow Instance.

## Code Setup

1.  Create the schema and table in MySQL
2.  Create below two connections,  
    a. aws_conn -> AWS Connection Credentials  
    b. rds_mysql -> MySQL RDS Credentials
3.  Place CSV files in the bucket under the path with pattern **my\_s3\_bucket/yyyy/mm/dd/hh/mm/file**.
4.  Update the s3 source location and MySQL schema-table info in the script.
5.  Place S3\_Mysql\_Load_Airflow.py in airflow dag folder and enable it in UI.
6.  Trigger the airflow dag or wait for the DAG to trigger (DAG is scheduled for every 1 hour)