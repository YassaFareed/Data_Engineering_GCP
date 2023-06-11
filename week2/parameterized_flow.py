from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into a pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix Data Type issues"""
    print(df.head())
    print(df.columns)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"Columns: {df.dtypes}")
    print(f"Rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as a parquet file"""

    # Create a folder data/green in the working directory before running this code
    path = Path(f"data/{color}/{dataset_file}.parquet")   
    df.to_parquet(path, compression="gzip")
    # Checking to see if the slashes are forward. Default is backwards
    print(path.as_posix())
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-de-course-388720")
    gcs_block.upload_from_path(from_path=path, to_path=path.as_posix()) # Using as_posix() to convert the slashes to forward
    return
    

@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The Main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz" 

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [3,4,5], year: int = 2021, color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, color)


# prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
# prefect deployment apply etl_parent_flow-deployment.yaml
# prefect agent start --work-queue "default" 


# - PREFECT: https://github.com/padilha/de-zoomcamp/tree/master/week2
# etl for cloud storage
# etl for gcs to big query
# parameterized flow: 
# > prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL" -> makes yaml for deployment where you can add your parameters
# > prefect deployment apply etl_parent_flow-deployment.yaml -> to apply the yaml deployment that was created
# prefect now knows it is ready to be run but says scheduled as it will not run (waiting for agent ). You can see it in work queue.
# We will start our agent locally. You can set it based on your use case (maybe kubenrnetes server). You need to go to deployment and click quick run and then use this command to start:
# >prefect agent start --work-queue "default" 
