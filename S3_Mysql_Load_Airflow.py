import io
from time import sleep
import ntpath
import os
import re
import shutil
from datetime import datetime, timedelta

import pandas as pd
from airflow.models.taskinstance import TaskInstance
from airflow import DAG
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "adithyapa",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

s3_file_pattern = r'\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/'

aws_conn_id = "aws_conn"
src_bucket = "lambda-bucket-bd"
src_prefix="source/"
mysql_conn_id = "rds_mysql"
schema = "billing_center"
table = "bctl_invoice"
archive_bucket = "lambda-bucket-bd"
archive_path ="archive/airflow/"

dag = DAG(
    dag_id="S3FilesToMysql",
    default_args=default_args,
    catchup=False,
    schedule_interval="@hourly",
    is_paused_upon_creation=True)





def generateS3Hook(aws_conn_id:str,**kwargs) -> S3Hook:
    """
    Generates S3 Connection Hook
    :param kwargs:
    :return: S3 Connection Hook
    """
    return S3Hook(aws_conn_id)


def getFileName(filePath):
    """
    Get the File Name from the path
    :param filePath: file name with path
    :return: return the filename
    """
    head, tail = ntpath.split(filePath)
    return tail or ntpath.basename(head)


def archiveS3Files(**kwargs):
    """
    Copy Bulk S3 Files to other location
    :param kwargs:
    :return:
    """
    keys = kwargs.keys()
    if ("trg_bucket" in keys and "trg_path" in keys and  "src_bucket" in keys ):
        src_bucket = kwargs["src_bucket"]
        trg_bucket = kwargs["trg_bucket"]
        s3_files = kwargs['ti'].xcom_pull(key="s3_data_files")
        s3_client = generateS3Hook(kwargs["aws_conn_id"])
        for file in s3_files.split(','):
            trg_path = str(kwargs["trg_path"]) + getFileName(file)
            s3_client.copy_object(source_bucket_key=file, dest_bucket_key=trg_path, source_bucket_name=src_bucket,
                                  dest_bucket_name=trg_bucket)
            sleep(0.5)
            s3_client.delete_objects(bucket=src_bucket,keys=file)
    else:
        raise Exception("Invalid Configuration")


def readS3FilesAndLoadtoMySql(**kwargs):
    """
    Read Data from S3 Files and load to Mysql
    :param kwargs:
    :return:
    """
    s3_files = fetchFilesBasedonPattern(**kwargs)
    
    tmp_trg_file_path = "/tmp/s3mysqlload_" + str(round(datetime.now().timestamp())) + "/"
    if s3_files is None:
        raise Exception("No Files are Available to process")
    else:
        files_df = pd.DataFrame()
        print(type(s3_files))
        data = ",".join(s3_files)
        kwargs['ti'].xcom_push(key='s3_data_files', value=data)
        s3_client = generateS3Hook(kwargs["aws_conn_id"])

        for path in s3_files:
            file_name = getFileName(path)
            if (file_name.lower().__contains__(".csv")):
                
                files_df = files_df.append(pd.read_csv(
                    io.BytesIO(s3_client.get_key(key=path, bucket_name=kwargs['src_bucket']).get()['Body'].read())))
            elif file_name.lower().__contains__(".json"):
                files_df = files_df.append(pd.read_json(
                    io.BytesIO(s3_client.get_key(key=path, bucket_name=kwargs['src_bucket']).get()['Body'].read())))

        if len(files_df) > 0:

            if not os.path.exists(tmp_trg_file_path):
                os.makedirs(tmp_trg_file_path)
            file_path = tmp_trg_file_path + str(round(datetime.now().timestamp())) + ".tsv"
            files_df.to_csv(file_path, sep="\t", index=False,header=False,line_terminator="\n")
            mysql_client = MySqlHook(mysql_conn_id=kwargs["mysql_conn"])
            mysql_client.bulk_load(table=kwargs["schema"]+"."+kwargs["table"], tmp_file=file_path)
            
            shutil.rmtree(tmp_trg_file_path)
        else:
            raise Exception("Source Files are Empty")


def fetchFilesBasedonPattern(**kwargs):
    """
    Fetch the file with a pattern
    :param kwargs:
    :return:
    """
    print(kwargs)
    xcom_data = kwargs["ti"]
    s3_files_paths_list = xcom_data.xcom_pull(key=None, task_ids="list_s3_files")
    print(s3_files_paths_list)
    if s3_files_paths_list:
        return [path for path in s3_files_paths_list if re.search(s3_file_pattern, path)]




list_s3_files = S3ListOperator(task_id="list_s3_files",
                               dag=dag,
                               aws_conn_id="aws_conn",
                               bucket=src_bucket, prefix=src_prefix)

load_s3_data_mysql = PythonOperator(task_id='load_s3_data_mysql',
                                    dag=dag,
                                    provide_context=True,
                                    python_callable=readS3FilesAndLoadtoMySql,
                                    op_kwargs={"aws_conn_id":aws_conn_id,'src_bucket': src_bucket, 'mysql_conn': mysql_conn_id,"schema":schema,"table":table})
copy_src_files_to_archive = PythonOperator(task_id="copy_src_files_to_archive",
                                           dag=dag,
                                           provide_context=True,
                                           python_callable=archiveS3Files,
                                           op_kwargs={'src_bucket': src_bucket,'trg_bucket':archive_bucket, 'trg_path': archive_path,"aws_conn_id":aws_conn_id}
                                           )

#delete_s3_files = S3DeleteObjectsOperator(task_id="delete_s3_files",
#                                          dag=dag,
#                                          aws_conn_id=aws_conn_id,
#                                          bucket=src_bucket,
#                                          keys="{{task_instance.xcom_pull(key='s3_data_files')}}".split(","))

list_s3_files >> load_s3_data_mysql >> copy_src_files_to_archive
# >> delete_s3_files
