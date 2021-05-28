from prefect import Flow, task
import json
import logging

from . import dbt_task


logging.basicConfig(level=logging.INFO)

class InvalidDagError(Exception):
    pass


class DBT:
    """ 
    Class to contain logic and metadata for working with a DBT project
    This can be used to integrate a DBT project with a Prefect flow, and create 
    a mapping of DBT models to Prefect Tasks.

    Args:
        - dbt_manifest (dict): A DBT projects manifest to be used to generate a 
            task mapping to a Prefect Flow

    """

    def __init__(self, manifest_path: str):
        with open(manifest_path) as f:
            self.dbt_manifest = json.load(f)
            self._parse_manifest()
            self.prefect_flow = self._set_prefect_flow()

    def _make_dbt_task(self, node: str, dbt_verb: str) -> dict:
        """
        Creates the appropriate shell command to run a DBT model, also sets 
        dependencies on other models

        Args:
            node (str): The name of the node/model parsed
            dbt_verb (str): Which DBT command to run, i.e 'dbt run' or 'dbt test'

        Returns:
            dict: Dictionary containing the command, and upstream dependencies
        """
        return {"command": f"dbt {dbt_verb} --models {node}", "upstream": []}

    def _parse_manifest(self) -> None:
        """
        Reads manifest dict object and generates all runtime commands, and
        dependencies between models
        """
        dbt_tasks = {}
        for node in self.dbt_manifest["nodes"].keys():
            if node.split(".")[0] == "model":
                model_name = node.split(".")[-1]
                dbt_tasks[node] = self._make_dbt_task(model_name, "run")

        # Identify all dependencies between models
        for node in self.dbt_manifest["nodes"].keys():
            if node.split(".")[0] == "model":
                for upstream_node in self.dbt_manifest["nodes"][node]["depends_on"][
                    "nodes"
                ]:
                    upstream_node_type = upstream_node.split(".")[0]
                    if upstream_node_type == "model":
                        dbt_tasks[node]["upstream"].append(upstream_node)

        self.dbt_tasks = dbt_tasks

    def _set_prefect_flow(self) -> Flow:
        """
        Converts dbt task dictionary into Prefect Flow
        """
        with Flow("sub_flow") as sub_flow:
            for key in self.dbt_tasks.keys():
                model_name = key
                run_command = self.dbt_tasks[model_name]["command"]
                upstream_tasks = self.dbt_tasks[model_name]["upstream"]

                inst = dbt_task.DBTTask(name=model_name)(
                    command=run_command, upstream_tasks=upstream_tasks
                )
                sub_flow.add_task(inst)

        return sub_flow

    def concat_tasks(self, prefect_flow: Flow, task_to_append_to: task = None) -> Flow:
        """
        Appends a Prefect Flow generated from a DBT project to an existing 
        Prefect Flow.

        By default this will append to the most-downstream task in
        a flow (If applicable), unless a specific task is specified - in which
        case the new flow will be inserted at this point in the DAG.

        Args:
            prefect_flow (Flow): Existing Prefect flow to append to
            task_to_append_to (task, optional): Specific Task in Flow to append 
                the new flow to. Defaults to None.

        Returns:
            Flow: A modified Prefect flow, containing all tasks from both Flows


        https://github.com/PrefectHQ/prefect/blob/master/src/prefect/core/flow.py
        """

        # Find last task in the DAG being passed

        if task_to_append_to is None:
            terminal_tasks = prefect_flow.terminal_tasks()
            if len(terminal_tasks) == 1:
                (terminal_task,) = terminal_tasks
                logging.info(f"Appending DBT tasks to terminal task: {terminal_task}")
            else:
                raise InvalidDagError(
                    "There is more than one terminal task in your Flow, please specify a task to append to."
                )

        else:
            logging.info("Appending to custom task...")

        return
