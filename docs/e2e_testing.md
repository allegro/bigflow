# Automated end-to-end testing

## Introduction

From this tutorial you will get to know how to create an e2e test for a workflow.
Automated e2e testing can help you develop workflows faster and avoid errors.

As an example, you will create an e2e test for the workflow that calculates aggregates on 
the `bigquery-public-data:crypto_bitcoin.transactions` BigQuery table. 
The workflow calculates:

* Daily transaction count
* Daily transaction fee sum 

The workflow saves the result in a BigQuery table.

You will write tests for two implementations of that workflow. The first implementation 
utilizes BigQuery (as the input and the output) and Dataflow (for processing).

The second implementation utilizes only BigQuery, for both io and processing.

Workflows using BigQuery are interesting to test, because there is no way emulate 
BigQuery on a local machine.

## Preparation

For this tutorial, we need a fresh BigFlow project.

First, create a directory where you want to store the example project:

```shell script
mkdir ~/bigflow_cookbook
cd ~/bigflow_cookbook
```

Now, prepare a virtual environment for the project. To do so, follow the BigFlow 
installation guide. The virtual environment should be placed inside 
the `~/bigflow_cookbook` directory.

Next, [create a new BigFlow project](scaffold.md) called "btc_aggregates":

```shell script
bf start-project
```

Install the project requirements:

```shell script
cd ~/bigflow_cookbook/btc_aggregates_project
pip install -r resources/requirements.txt
```

Finally, remove examples from the generated project:

```shell script
cd ~/bigflow_cookbook/btc_aggregates_project
rm -rf btc_aggregates/internationalports
rm -rf btc_aggregates/wordcount
```

## Testing Dataflow + BigQuery implementation

```python

```

## Testing BigQuery implementation

## Summary

