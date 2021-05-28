from prefect import Flow, task, Parameter, Task
import prefect.utilities
import pathlib
import os
import json
import subprocess

logger = prefect.utilities.logging.get_logger(__name__)

# --------------------------------- Helpers! -----------------------------------#
class DBTTask(Task):
    def run(self, command):
        logger.info(f"Running {command}")
        subprocess.call(command, shell=True)


def make_dbt_task(node: str, dbt_verb: str) -> dict:
    return {"command": f"dbt {dbt_verb} --models {node}", "upstream": []}


# ----------------------------------- Flow -----------------------------------=-#


@task
def flow_init(stage: str, account: str, credentials: str) -> dict:
    variables = {"stage": stage, "account": stage, "credentials": credentials}

    return variables


@task
def load_manifest() -> dict:
    if os.environ.get("DWAAS_ENV") == "AWS":
        manifest_path = "./tm_dbt/target/manifest.json"
    else:
        manifest_path = "../../tm_dbt/target/manifest.json"

    with open(manifest_path) as f:
        data = json.load(f)

    return data


@task
def process_mani(data: dict) -> dict:
    """ Reads manifest.json data, and creates an identical DAG of Prefect tasks

    Args:
        data (dict): Raw JSON data from mainfest.json

    Returns:
        dict: [description]
    """

    # Identify all models
    dbt_tasks = {}
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            model_name = node.split(".")[-1]
            dbt_tasks[node] = make_dbt_task(model_name, "run")

    # Identify all dependencies between models
    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_tasks[node]["upstream"].append(upstream_node)

    return dbt_tasks


@task
def create_sub_dag(dbt_tasks: dict) -> None:
    with Flow("subflow") as sub_flow:
        for key in dbt_tasks.keys():
            model_name = key
            run_command = dbt_tasks[model_name]["command"]
            upstream_tasks = dbt_tasks[model_name]["upstream"]

            inst = DBTTask(name=model_name)(
                command=run_command, upstream_tasks=upstream_tasks
            )
            sub_flow.add_task(inst)

    wd = os.getcwd()
    os.chdir("../../tm_dbt/")
    sub_flow.run()

    return None


with Flow("dbt-integration-test") as flow:
    stage = Parameter("stage", default="test")
    account = Parameter("account", default="692350155389")
    credentials = Parameter("credentials", default=None)

    variables = flow_init(stage, account, credentials)

    mani = load_manifest()
    nodes = process_mani(mani)

    subdag = create_sub_dag(nodes)

