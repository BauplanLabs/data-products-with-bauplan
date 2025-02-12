import time
import uuid
import tempfile
import bauplan
import os
import subprocess


# some constants - make sure to use the correct bucket and git repo ;-)
MY_BUCKET = 'hello-data-products-with-bauplan'
DATA_FOLDER = 'raw'
CODE_REPO_URL = 'https://github.com/BauplanLabs/bauplan-data-products-preview'


def _add_mock_data_to_table(
    data_folder: str
):
    # we relativize the imports because this function is a mock 
    # simulating an outside system, so these dependencies are not
    # really needed in the data product main code
    import boto3
    s3 = boto3.client('s3')
    import pyarrow as pa
    import pyarrow.parquet as pq
    arrow_table = pa.Table.from_pylist(cleaned_records)
    # write the table to a temporary parquet file and upload to S3
    with tempfile.NamedTemporaryFile() as tmp:
        pq.write_table(arrow_table, tmp.name)
        s3.upload_file(tmp.name, MY_BUCKET, f"{data_folder}/{str(uuid.uuid4())}.parquet")
    
    return 
    

# the lambda handler function, triggered on a schedule
def lambda_handler(event, context):
    start = time.time()
    ### 1: WE ADD SOME SIMULATED DATA TO THE TABLE AS INPUT PORT ###
    # In a real-world scenario, this would be the upstream data product
    # producing new data in the agreed table
    n_records = _add_mock_data_to_table(DATA_FOLDER)
    
    
    ### 2: WE GET THE LATEST TRANSFORMATION CODE FROM GITHUB ###
    # We get the code from git,
    # so we can run the very latest version of the data product logic
    # (it can be customized to branches or tags etc.)
    with tempfile.TemporaryDirectory() as tmpdirname:
        print("Getting the latest code from the repository")
        repo_path = os.path.join(tmpdirname, "repo")
        subprocess.check_call(["git", "clone", CODE_REPO_URL, repo_path])
        # make sure the files are in the right place, check for README.md
        assert os.path.exists(os.path.join(repo_path, "README.md")), "No README.md found in the repository"
        print(f"Repository cloned correctly to {repo_path}")
        ### 3: WE TRIGGER THE DATA PRODUCT LOGIC ###
        # We get the code from git (can be customized to branches or tags etc.)
        # and run the data product logic with the Bauplan SDK
        # The output will be the table specified as output port in the shared
        # data product configuration



    end = time.time()
    # store in Cloudwatch the total number of records processed
    print({
        "metadata": {
            "timeMs": int((end - start) * 1000.0),
            "epochMs": int(end * 1000),
            "eventId": str(uuid.uuid4()),
        },
        "data": {
            "totalNewRecords": n_records
        }
    })

    return True