from prefect import flow, task
from src.pipeline import main as run_pipeline

@task
def run_full():
    run_pipeline()

@flow(name="data_pipeline")
def etl_flow():
    run_full()

if __name__ == "__main__":
    etl_flow()