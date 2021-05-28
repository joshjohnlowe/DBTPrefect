from prefect import Flow, task

from dbt_prefect.dbt import DBT


dbt_flow = DBT(manifest_path="./manifest.json")


# Define a very simple prefect flow
@task
def print_stuff():
    print("Wow!")
    return "nice"


@task
def second_task(x):
    print(x)


with Flow("test-flow") as test_flow:
    a = print_stuff()
    b = second_task(a)



dbt_flow.concat_tasks(test_flow)