Building and deploying Sleeper locally
======================================

You can also run a LocalStack container locally and deploy an instance of Sleeper to it. This deployment method has
limited
functionality, but will allow you to peform a queue-based standard ingest, and run compactions on a single table.

## Prerequesites

Before you can deploy Sleeper in a LocalStack container you need to install `docker` and `docker-compose`.

## Launch LocalStack container

To launch the LocalStack container, run the `startContainer.sh` script in the `scripts/deploy/LocalStack` folder.
This will also output commands you can use to point Sleeper scripts to your LocalStack container.

## Deploy to LocalStack

For Sleeper commands to interact with LocalStack, ensure that the `AWS_ENDPOINT_URL` environment variable
is set. Commands to do this are provided by the `startContainer.sh` script, but you can also manually set this by
running the following command:

```shell
export AWS_ENDPOINT_URL=http://localhost:4566
```

To go back to using the default AWS endpoint, you can unset this environment variable:

```shell
unset AWS_ENDPOINT_URL
```

Before you can run any LocalStack scripts, you need to build the project. You can do this by running the
following command:

```shell
./scripts/build/build.sh
```

To deploy an instance of Sleeper to your LocalStack container, you can run the following command in the
`scripts/deploy/LocalStack` folder. Note that you will not be able to run this command unless you have the
AWS_ENDPOINT_URL environment variable set as described in the previous section.

```shell
./deploy.sh <instance-id>
```

This will create a config bucket and a table bucket in LocalStack, and upload the necessary properties files.
A single table will be created with the name `system-test`.

Once the instance is deployed, you can launch the admin client to view the instance and table properties of the
instance, as well as running partition and file status reports.

```shell
./scripts/utility/adminClient.sh <instance-id>
```

## Tear down instance

You can tear down an existing instance by running the following command in the `scripts/deploy/LocalStack` folder.

```shell
./tearDown.sh <instance-id>
```

## Stop LocalStack container

To stop the LocalStack container, you can run the following command in the `scripts/deploy/LocalStack` folder.

```shell
./stopContainer.sh
```