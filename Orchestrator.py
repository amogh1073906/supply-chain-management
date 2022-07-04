# Databricks notebook source
# Errors in workflows thrown a WorkflowException.

def run_notebook(notebook, timeout):
    try:
        return dbutils.notebook.run(notebook, timeout)
    except Exception as e:
        raise e

if run_notebook("mount", 0) == "Mounted Successfully":
    if run_notebook("USECASE1", 0) == "UseCase1 Notebook successfully run!!":
        if run_notebook("USECASE2", 0) == "UseCase2 Notebook successfully run!!":
            dbutils.notebook.exit("WORKFLOW COMPLETED!!!")
