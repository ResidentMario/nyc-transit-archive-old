# nyc-transit-archive

The NYC Transit Archive service is managed using a set of task graphs (or DAGs) running inside of [Apache Airflow](https://airflow.apache.org/).

## Development

### Running tasks locally

This section documents the steps required to run the tasks locally.

First, create a new environment (the following uses `conda`, but you can also use something else, e.g. `pipenv`) with the requisite packages installed:

```bash
# the attrs library may fail to install if you do not prepulate the environment with Python
conda create --name quilt-gtfs-rt-pipeline-dev python=3.6
conda activate quilt-gtfs-rt-pipeline-dev

# install script/dev env packages
conda install jupyter boto3

# install airflow
export AIRFLOW_HOME=~/airflow
pip install apache-airflow

# install the cryptography lib
pip install cryptography
```

If you haven't done so already, clone this repo:

```bash
git clone https://github.com/ResidentMario/nyc-transit-archive.git
```

Next you will need to generate a Fernet key. The following code will copy one to your clipboard.

```bash
python -c "from cryptography.fernet import Fernet; fernet_key= Fernet.generate_key(); print(fernet_key.decode())" | pbcopy
```

Now edit `~/airflow/airflow.cfg`. Make the following edits:

* Set `sql_alchemy_conn` to `sqlite:////Users/alex/airflow/airflow.db`.
* Set `load_examples` to `False`.
* Set `fernet_key` to the value you just pasted to clipboard.
* Set `executor` to `SequentialExecutor`.
* Set `dags_folder` to `~/Desktop/quilt-airflow/dags/`.

Next, export your MTA access key to the `mtakey` environment variable:

```bash
export mtakey=$KEY_VALUE
```

If you do not have an access key you will need to create one. You can do so [on the MTA website](https://datamine.mta.info/user/register).

Make sure that you are authenticated for AWS. You will need to change the buckets you write to in the various tasks in the `dags/` folder to ones that you have access to.

Start the scheduler process:

```bash
airflow scheduler
```

Start the webserver process:

```bash
airflow webserver
```

Navigate to `localhost:8080` and you're ready to go.
