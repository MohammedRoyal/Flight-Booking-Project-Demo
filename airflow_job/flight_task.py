from datetime import datetime,timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flight_data_processing',
    default_args=default_args,
    description='A DAG to process flight data using Dataproc',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    env=Variable.get("env",default_args='dev')
    gcp_bucket=Variable.get("gcp_bucket",default_args='airflow_demo_data')
    bq_dataset=Variable.get("bq_dataset",default_args=f'flight_data_{env}')
    bq_project=Variable.get("bq_project",default_args='neon-effect-473212-d7')
    tables=Variable.get("tables",deserialize_json=True)

    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    job_batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"

    file_sensor = GCSObjectExistenceSensor(
        task_id='file_sensor',
        bucket=gcp_bucket,
        object=f'fighlt_data/source-{env}/flight_booking.csv',
        poke_interval=60,
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode 
    )

    batch_details = {
        "pyspark_batch":{
            "main_python_file_uri":f"gs://{gcp_bucket}/fighlt_data/spark_job/flight_job.py",
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args":[
                f"--env={env}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
                f"--bq-project={bq_project}"

            ]
        },
          "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "databricks-compute@neon-effect-473212-d7.iam.gserviceaccount.com",
                "network_uri": f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri": f"projects/{bq_project}/regions/us-central1/subnetworks/default",
            }
        },
    }

    create_batch=DataprocCreateBatchOperator(
        task_id="create_batch",
        batch=batch_details,
        batch_id=job_batch_id,
        project_id=bq_project,
        region="us-central1",
        gcp_conn_id="google_cloud_default",    
        
    )

    file_sensor >> create_batch



