# bauplan-data-products-preview
Playground for data product blog post with Bauplan - will be moved to a stable place once done

## Overview

[Bauplan](https://www.bauplanlabs.com/) is the easiest and fastest way to build data products over object storage. 

This is a reference implementation for a data product according to the best practices in [Managing Data as a Product](https://github.com/PacktPublishing/Managing-Data-as-a-Product), and showcases a self-standing data product running on a AWS Lambda with Bauplan.

_Quick links_:

* For more context and background, check out the companion blog post LINK HERE.
* A video walkthrough of the system running from our laptop is LINK HERE.

## Data flow

This reference data product takes as input a table containing taxi trips with toll, tip and total USD amount, and produces as output a table with the averages for each date, certified for data quality through tests. The data flow for this reference implementation is as follows:

* the `data-product-descriptor.json` in the repo contains the configuration for the data product;
* the AWS Lambda (`handler.py` in the `serverless` directory) is used to first simulate the input port for the product - it does so by mocking trips data and loading them into Bauplan through the `bauplan` Python package;
* once the input port is thus populated, the handler kicks off the computation of the actual data product: it downloads the latest version of the transformation code (a [Bauplan DAG](https://docs.bauplanlabs.com/en/latest/concepts/models.html)) from GitHub, and runs it from the input port on a temporary [branch](https://docs.bauplanlabs.com/en/latest/concepts/branches.html); sincen `data-product-descriptor.json` is part of the repo in this case, the configuration is also downloaded and used to guide the computation with some dynamic parameters;
* the output table is verified for data quality through tests: the declarative tests in the configuration gets translated on the fly to Bauplan [expectations](https://docs.bauplanlabs.com/en/latest/examples/expectations.html) and run on the output table;
* if the tests pass, the output table is certified and the data product is considered ready for downstream consumer: the branch is merged into the output port branch as specified in the configuration; if the tests fail, the output port is not updated and the temporary branch is left open for inspection.

The entire system runs on a trigger or on a schedule as a self-contained, serverless system that does not need any infrastructure, data copy, sync between environments, or manual intervention.

## Setup

* If you don't have a Bauplan key for the free sandbox, require one [here](https://www.bauplanlabs.com/#join). Complete the [3 min tutorial](https://docs.bauplanlabs.com/en/latest/tutorial/index.html) to check your setup and get familiar with the platform.
* We assume you have an AWS account, and local credentials properly configured and compatible with launching Lambdas.
* We assume you have Docker installed in your laptop (it will be used to build the Lambda image).
* We use the [serverless framework](https://www.serverless.com/framework) to manage Lambda deployment as code, but the same lambda could be deployed manually if you prefer to go through the AWS console. Make sure the framework is installed and properly configured to use the proper AWS credentials.

## How to run it

Change the `bauplan_user` into the `serverless.yml` to reflect your Bauplan username. Then setup your Bauplan key as an environment variable:

```bash
export BAUPLAN_KEY=<your_bauplan_key>
```

Now, you can cd into the `serverless` directory and deploy the lamnda with the serverless framework:

```bash
cd serverless
serverless deploy
```

That's it! The lambda is now deployed and ready to run. You can trigger it manually from the AWS console, or wait for the schedule (as specified in the `serverless.yml`) to kick in. Every time the lambda runs, the above data flow is executed, and the output table is updated with the latest averages for the taxi trips.

NOTE: if you wish to test the data transformation logic in isolation and interactively, you can use the Bauplan CLI as you would normally do with any DAG (the Bauplan sandbox in fact contains a copy of the input port data in the `main` branch):

```bash
python3 -m venv venv
source venv/bin/activate
pip install bauplan --upgrade
cd src/bpln_pipeline
bauplan run --namespace tlc_trip_record --dry-run --preview head
```

## Where to go from here?

TBC

## License

This reference implementation is released under the MIT License. Bauplan is built by [BauplanLabs](https://www.bauplanlabs.com/), all rights reserved.