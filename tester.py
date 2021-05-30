from prefect import Flow, task

from dbt_prefect.dbt import DBT


dbt_flow = DBT(manifest_path="./manifest.json")


# Define a very simple prefect flow
@task
def prefect_task_one():
    print("Wow!")
    return "nice"


@task
def prefect_task_two(x):
    print(x)
    return "AAA"


@task(name="third_task")
def prefect_task_three(x):
    print(x)



with Flow("test-flow") as test_flow:
    a = prefect_task_one()
    b = prefect_task_two(a)
    c = prefect_task_three(b)



test_flow.visualize()




dbt_flow.join_flow(prefect_flow=test_flow, task_to_append_to=prefect_task_three).visualize()


""" 
testing scenarios 

Prefect flow with more than one terminal task 
- A "joining" task MUST be specified

DBT flow with more than one root task 
- Each root task in the DBT flow must have an upstream dependancy 
    on the Prefect flows specified "joining" task
"""