import time
import uuid
import tempfile
import bauplan
import os
import subprocess
from datetime import datetime, timezone


bpln_client = bauplan.Client(api_key=os.environ['bauplan_key'])
bpln_user = os.environ['bauplan_user']
print(f"Bauplan client created for user {bpln_user}")
# some constants - make sure to use the correct bucket and git repo ;-)
MY_BUCKET = 'hello-data-products-with-bauplan'
DATA_FOLDER = 'raw'
CODE_REPO_URL = 'https://github.com/BauplanLabs/bauplan-data-products-preview'
GB_PER_ITERATION = 0.2
NUMERICAL_COLUMNS = [ 'Tip_amount', 'Tolls_amount']
# input port vars are the same as the JSON configuration - they are included
# here for the mock generation of the streaming of new data
INPUT_PORT_TABLE = 'tripsTable'
INPUT_PORT_NAMESPACE = 'tlc_trip_record'


def _add_mock_data_to_input_port(
    bpln_client,
    bpln_user: str,
    bucket: str,
    data_folder: str,
    formatted_date_as_string: str, # the "current" date to use in the mock trips
    gb_per_iteration: float,
    numerical_columns: list, # list of numerical columns to generate,
    input_port_table: str,
    input_port_namespace: str,
):
    # we relativize the imports because this function is a mock 
    # simulating an outside system, so these dependencies are not
    # really needed in the data product main code
    import boto3
    s3 = boto3.client('s3')
    import pyarrow as pa
    import pyarrow.parquet as pq
    import numpy as np
    
    n_columns = len(numerical_columns)
    rows = int(gb_per_iteration * 1024**3 / n_columns / 8)
    cols = [np.random.randint(1, 10, rows) for _ in range(n_columns)]
    total_col = np.array(cols).sum(axis=0)
    all_cols = cols + [total_col] + [[formatted_date_as_string for _ in range(rows)]]
    t = pa.Table.from_arrays(all_cols, names=numerical_columns + ['Total_amount', 'tpep_pickup_datetime'] )
    # using a temporary file, do a WAP ingestion into the table
    with tempfile.NamedTemporaryFile() as tmp:
        pq.write_table(t, tmp.name)
        file_name = f'{str(uuid.uuid4())}.parquet'
        s3.upload_file(tmp.name, bucket, f"{data_folder}/{file_name}")
        s3_uri = f"s3://{bucket}/{data_folder}/{file_name}"
        ### A: start an ingestion branch
        ingestion_branch = f'{bpln_user}.ingestion_{str(uuid.uuid4())}'
        # clean up the branch if it exists - as a demo, we are making
        # all the code stateless and easier to run
        if bpln_client.has_branch(ingestion_branch):
            bpln_client.delete_branch(ingestion_branch)
            
        bpln_client.create_branch(ingestion_branch, 'main')
        print("Branch created!")
        ### B: create (or replace) the table in Bauplan
        # note: being a mock of an input port, we are replacing the table
        # everytime to make the demo stateless and easier to run - this
        # choice does not affect in any way downstream data logic
        tbl = bpln_client.create_table(
            table=input_port_table,
            search_uri=s3_uri,
            branch=ingestion_branch,
            namespace=input_port_namespace,
            replace=True
        )
        print("Table created!")
        ### C: append the data
        plan_state = bpln_client.import_data(
            table=input_port_table,
            search_uri=s3_uri,
            branch=ingestion_branch,
            namespace=input_port_namespace
        )
        if plan_state.error:
            raise RuntimeError(f"Error ingesting data: {plan_state.error}")
        print("Data ingested!")
        ### D: merge the branch
        # note that the product configuration mentions the branch in which
        # to find the data, in this case main
        bpln_client.merge_branch(
            source_ref=ingestion_branch,
            into_branch='main',
        )
        print("Branch merged!")
        bpln_client.delete_branch(ingestion_branch)
        print("Branch deleted!")
    
    return rows
    

# the lambda handler function, triggered on a schedule
def lambda_handler(event, context):
    start = time.time()
    
    ### 0: at the start of the function (which runs on a schedule)
    # we get the current date as a string DD/MM/YYYY - this will be used
    # as the trip date for the mock data in the input port, as well as the
    # parameter to trigger the transformation logic in the data product
    formatted_date_as_string = datetime.now(timezone.utc).strftime('%d/%m/%Y')
    
    ### 1: WE ADD SOME SIMULATED DATA TO THE TABLE AS INPUT PORT ###
    # In a real-world scenario, this would be the upstream data product
    # producing new data in the agreed table
    n_records = _add_mock_data_to_input_port(
        bpln_client,
        bpln_user=bpln_user,
        bucket=MY_BUCKET,
        data_folder=DATA_FOLDER,
        formatted_date_as_string=formatted_date_as_string,
        gb_per_iteration=GB_PER_ITERATION,
        numerical_columns=NUMERICAL_COLUMNS,
        input_port_table=INPUT_PORT_TABLE,
        input_port_namespace=INPUT_PORT_NAMESPACE,
    )
    
    ### 2: WE GET THE LATEST TRANSFORMATION CODE FROM GITHUB ###
    # We get the code from git,
    # so we can run the very latest version of the data product logic
    # (it can be customized to branches or tags etc.)
    with tempfile.TemporaryDirectory() as tmpdirname:
        print("Getting the latest code from the repository")
        repo_path = os.path.join(tmpdirname, "repo")
        subprocess.check_call(["git", "clone", CODE_REPO_URL, repo_path])
        # make sure the files are in the right place, check for data-product-descriptor.json
        assert os.path.exists(os.path.join(repo_path, "data-product-descriptor.json")), "data-product-descriptor.json not found in the repository"
        print(f"Repository cloned correctly to {repo_path}")
        ### 3: WE TRIGGER THE DATA PRODUCT LOGIC ###
        # We get the code from git (can be customized to branches or tags etc.)
        # and run the data product logic with the Bauplan SDK
        # The output will be the table specified as output port in the shared
        # data product configuration
        pipeline_project_path = os.path.join(repo_path, "src", "bpln_pipeline")
        sandox_branch = f'{bpln_user}.sandbox_{INPUT_PORT_TABLE}_{str(uuid.uuid4())}'
        # 3.a: create a sandbox branch to run the pipeline SAFELY and check the data quality
        # before making it available in the output port (i.e. merging it into main)
        
        # note: since it's a demo, we are deleting the branch if it exists, to make
        # the code stateless and easier to run
        if bpln_client.has_branch(sandox_branch):
            bpln_client.delete_branch(sandox_branch)
            
        bpln_client.create_branch(sandox_branch, 'main')
        # make sure to catch any error and delete the branch if something goes wrong
        try:
            print("Running the pipeline")
            run_state = bpln_client.run(
                project_dir=pipeline_project_path,
                branch_name=sandox_branch,
                namespace=INPUT_PORT_NAMESPACE,
                #parameters={},
                client_timeout=500
            )
            print(f"Pipeline run, id: {run_state.job_id}")
            # if all goes well, we merge the branch into main
            # as the output port of the data product
            bpln_client.merge_branch(
                source_ref=sandox_branch,
                into_branch='main',
            )
            print(f"Branch {sandox_branch} merged into main!")
            # finally, we delete the temoporart branch
            bpln_client.delete_branch(sandox_branch)
            print(f"Branch {sandox_branch} deleted!")
        except Exception as e:
            # if something goes wrong, we do NOT merge the branch
            # to avoid giving consumers of the data product bad data
            # and we do NOT delete the branch, so we can inspect the
            # state of the pipeline!
            # for now, let's just print the error
            print(f"Error: {e}")
            print(f"Branch {sandox_branch} was NOT deleted!")

    end = time.time()
    # store in Cloudwatch the total number of records processed
    print({
        "metadata": {
            "timeMs": int((end - start) * 1000.0),
            "epochMs": int(end * 1000),
            "eventId": str(uuid.uuid4()),
        },
        "data": {
            "totalNewRows": n_records
        }
    })

    return True