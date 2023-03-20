from prefect import task, flow
import pandas as pd
from urllib.parse import urlparse


@task
def load_data() -> pd.DataFrame:
    data = {'id': ['1', '2'], 'name': ['hello', 'world'], 'url': ['http://hello.com/home', 'https://world.org/']}
    # Create DataFrame
    df = pd.DataFrame(data)
    return df


@task
def add_domain(df: pd.DataFrame) -> pd.DataFrame:
    df['domain_of_url'] = df['url']
    df['domain_of_url'] = df['domain_of_url'].apply((lambda x: urlparse(x).netloc))
    return df


@task
def copy_to_file(df: pd.DataFrame, path: str):
    df.to_csv(path)


@flow(name="task_prefect")
def my_flow(path_in, path_out):
    data = load_data()
    data_domains = add_domain(df=data)
    copy_to_file(df=data_domains, path=path_out)