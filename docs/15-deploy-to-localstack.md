Building and deploying Sleeper locally
======================================

You can also run a localstack container locally and deploy an instance of Sleeper to it. This deployment method has
limited
functionality, but will allow you to peform a queue-based standard ingest, and run compactions on a single table.

## Prerequesites

Before you can deploy sleeper in a localstack container you need to install `docker` and `docker-compose`.

## Launch Localstack container

To launch the localstack container, run the `startContainer.sh` script in the `scripts/deploy/localstack` folder.
This will also output commands you can use to point Sleeper scripts to your localstack container.

## Deploy to Localstack

For Sleeper commands to interact with localstack, ensure that the `AWS_ENDPOINT_URL` environment variable
is set. Commands to do this are provided by the `startContainer.sh` script, but you can also manually set this by
running the following command:

```shell
export AWS_ENDPOINT_URL=http://localhost:4566
```

To go back to using the default AWS endpoint, you can unset this environment variable:

```shell
unset AWS_ENDPOINT_URL
```

To deploy an instance of sleeper to your localstack container, you can run the following command in the
`scripts/deploy/localstack` folder. Note that you will not be able to run this command unless you have the
AWS_ENDPOINT_URL environment variable set as described in the previous section.

```shell
./deploy.sh <instance-id>
```

This will create a config bucket and a table bucket in localstack, and upload the necessary properties files.
A single table will be created with the name `system-test`.

Once the instance is deployed, you can launch the admin client to view the instance and table properties of the
instance, as well as running partition and file status reports.

```shell
./scripts/utility/adminClient.sh <instance-id>
```

## Standard ingest
To ingest some data into the `system-test` table in your instance, you can run the following script in the 
`scripts/deploy/localstack` folder.
```shell
./ingestFiles <instance-id> <file1.parquet> <file2.parquet> <file3.parquet> ....
```
This script will upload the provided files to an ingest source bucket in localstack, create ingest jobs, and 
send them to the ingest job queue. It will then build the ingest-runner docker image, and launch a container for it, 
which will take the ingest job off the queue and perform the ingest.

You can then view the ingest jobs and task that were run by launching the admin client and running an ingest job or 
ingest task status report

## Tear down instance

You can tear down an existing instance by running the following command in the `scripts/deploy/localstack` folder.

```shell
./tearDown.sh <instance-id>
```

## Stop Localstack container

To stop the localstack container, you can run the following command in the `scripts/deploy/localstack` folder.

```shell
./stopContainer.sh
```