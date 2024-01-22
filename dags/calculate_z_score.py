from uuid import uuid4
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset as AirflowDataset
from airflow.operators.python import PythonOperator
import pandas as pd

from openlineage.client.run import Run, RunState, RunEvent, InputDataset, Job
from openlineage.client.run import Dataset as OpenlineageDataset
from openlineage.client import OpenLineageClient

from datetime import datetime

from model1 import calculate_statistics
from model2 import calculate_z_score

import requests
import json

url = "http://host.docker.internal:5000/api/v1/lineage"
ol_url = "http://host.docker.internal:5000"
dataset = AirflowDataset("data/credit-data.csv")
# runId = str(uuid4())
# run = Run(runId)
ol_client = OpenLineageClient(ol_url)
print(ol_client)

namespace = "DataLineage"

job = Job(namespace=namespace, name="DataLineage.calculate_z_score")

credit = OpenlineageDataset(namespace=namespace, name="DataLineage.credit")
avg_age = OpenlineageDataset(namespace=namespace, name="DataLineage.avg_age")
std_age = OpenlineageDataset(namespace=namespace, name="DataLineage.std_age")
z_age = OpenlineageDataset(namespace=namespace, name="DataLineage.z_age")
producer = "MathWorks.com"
headers = {'Content-Type': 'application/json'}


def _model1_calc_stats():

    job = "DataLineage.calc_mean_std"
    runId = str(uuid4())
    run = Run(runId)

    # inform marquze of start
    payload_start = json.dumps({
        "eventType": "START",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
            "runId": runId
        },
        "job": {
            "namespace": namespace,
            "name": job
        },

        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
    })
    response = requests.request(
        "POST", url, headers=headers, data=payload_start)

    data = pd.read_csv(dataset.uri)
    mean_age, std_age = calculate_statistics(data)

    # inform marquez of completion
    payload_complete = json.dumps({
        "eventType": "COMPLETE",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
            "runId": runId
        },
        "job": {
            "namespace": namespace,
            "name": job
        },

        "inputs": [
            {
                "namespace": namespace,
                "name": "credit-data"
            }],

        "outputs": [{
            "namespace": namespace,
            "name": "mean-age",
        },
            {
            "namespace": namespace,
            "name": "std-age",
        }],

        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",

        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

    })
    response = requests.request(
        "POST", url, headers=headers, data=payload_complete)

    return mean_age, std_age


def _model2_z_score(ti):

    job = "DataLineage.calc_z_score"
    runId = str(uuid4())

    payload_start = json.dumps({
        "eventType": "START",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
            "runId": runId
        },
        "job": {
            "namespace": namespace,
            "name": job
        },

        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",

        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

    })
    response = requests.request(
        "POST", url, headers=headers, data=payload_start)

    response = ti.xcom_pull()
    mean_age, std_age = response

    data = pd.read_csv(dataset.uri)
    z_scores = calculate_z_score(data, mean_age, std_age)

    payload_complete = json.dumps({
        "eventType": "COMPLETE",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {
            "runId": runId
        },
        "job": {
            "namespace": namespace,
            "name": job
        },

        "outputs": [
            {
                "namespace": namespace,
                "name": "z-score"
            }],

        "inputs": [{
            "namespace": namespace,
            "name": "mean-age",
        },
            {
            "namespace": namespace,
            "name": "std-age",
        }],

        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",

        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

    })
    response = requests.request(
        "POST", url, headers=headers, data=payload_complete)

    print(z_scores)
    return z_scores


with DAG(
    "pandas_write_and_read", start_date=datetime(2021, 12, 1), catchup=False
):

    model1_calc_stats = PythonOperator(
        task_id="model1_calc_stats", python_callable=_model1_calc_stats
    )

    model2_z_score = PythonOperator(
        task_id="model2_z_score", python_callable=_model2_z_score
    )

    model1_calc_stats >> model2_z_score
