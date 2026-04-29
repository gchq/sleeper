Building and deploying Sleeper
==============================

This contains instructions on how to deploy Sleeper.

Please follow the [getting started guide](getting-started.md) to install or build Sleeper and its dependencies, and
prepare your AWS account. For the rest of this guide we'll assume you're working in an EC2 instance in an AWS account
that's configured appropriately. You should either have the dependencies listed there installed in your EC2, or run a
builder Docker container in your EC2 with the [Sleeper Docker tools](deployment/docker-tools.md), which comes with those
dependencies pre-installed.

If you just want a local instance for testing, see the documentation
on [deploying to localstack](deployment/deploy-to-localstack.md). This has very limited functionality compared to a
deployed instance.

## Deployment

Sleeper is deployed using the AWS CDK. You can invoke the CDK to do this either using the automated scripts or by using
the CDK directly. The scripts do the same thing as the direct CDK deployment, but through Java and with some
configuration specific to either creation or update of an existing instance. The scripts also include configuration of
tables, which direct CDK deployment does not.

Either approach should be done from within an EC2 instance to avoid lengthy uploads of large jar files and Docker
images.

### Using the CDK directly

If you prefer to use the CDK CLI directly for deployment, or you want to include Sleeper in your own CDK app, see
[Deployment with the CDK](deployment/deploy-with-cdk.md) for more information.

### Scripted deployment

The scripts for deployment create an instance of Sleeper either from your own configuration files, or from templates.

The two scripts available to use are found in scripts/deploy and have the following usecases:

| File              | Purpose                                 | Use case |
|-------------------|-----------------------------------------|----------|
| deployNew.sh      | Deploys a new instance                  | Use when deploying a new instance
| deployExisting.sh | Upgrades/redeploys an existing instance | Use to upgrade an instance to a newer version, or when you wish to re-run the CDK against an existing deployment

They also upload the necessary deployment artefacts to AWS. These artefacts must be available for deployment, either by
building Sleeper locally or installing pre-published artefacts. See the [developer guide](developer-guide.md#building)
for details. These artefacts will be uploaded to a separate CDK stack from the Sleeper instance.

The required configuration files will be copied or written to a folder called `generated` in the scripts directory, to
be read by the CDK.

#### From templates

You can find the template files [here](../scripts/templates). It is recommended that you change these templates to
configure Sleeper in the way that you want before you run the automated script. At the very least you will want to
change the tags.template file. See the Configuration section below for further details.

If you deploy from the templates, it will create an instance with no tables:

```bash
cd scripts
editor templates/instanceproperties.template
editor templates/tags.template
./deploy/deployNew.sh <instance-id> <vpc-id> <subnet-ids>
```

Here `vpc-id` and `subnet-ids` are the ids of the VPC and subnets that some components of Sleeper will be deployed into.
Multiple subnet ids can be specified with commas in between, e.g. `subnet-a,subnet-b`.

#### From configuration files

You can create your own configuration for a Sleeper instance, including tables, and deploy that. See
the [configuration documentation](deployment/instance-configuration.md) for more details. These commands use the basic
example as a starting point:

```bash
mkdir scripts/my-instance
cp example/basic/* scripts/my-instance/
# Edit all configuration files in the new directory to set your own values
./scripts/deploy/deployNew.sh <instance-id> <vpc-id> <subnet-ids> ./my-instance/instance.properties
```

#### Upgrade/redeploy existing instance

The `deployExisting.sh` script can be used to re-run the CDK against an existing instance of Sleeper.
This can be used to upgrade an instance of Sleeper to a newer version.
This will upload any jars that have changed, update all the docker images, and perform a `cdk deploy`.

```bash
./scripts/deploy/deployExisting.sh <instance-id>
```

## Interacting with and editing an instance

There are clients and scripts in the `scripts/deploy` and `scripts/utility` directories that can be used to adjust an
existing instance.

See the [usage guide](usage-guide.md) for information on how to interact with the instance. The admin client described
there can be used to adjust the configuration of an instance by setting instance properties.

See the [tables documentation](usage/tables.md#addedit-a-table) for how to add/edit Sleeper tables.

### Pausing and restarting the system

If there is no ingest in progress, and all compactions have completed, then Sleeper will go to sleep, i.e. the only
significant ongoing charges are for data storage. However, there are several lambda functions that are scheduled to
run periodically using EventBridge rules. These lambda functions look for work to do, such as compactions to run.
The execution of these should have very small cost, but it is best practice to pause the system,
i.e. turn these rules off, if you will not be using it for a while. Note that the system can still be queried when
it is paused.

```bash
# Pause the System
./scripts/utility/pauseSystem.sh ${INSTANCE_ID}

# Restart the System
./scripts/utility/restartSystem.sh ${INSTANCE_ID}
```

### Tear down

If you deployed Sleeper with the scripts or the Sleeper CDK apps, and you still have the `generated` folder in the
project root directory, you can run:

```bash
./scripts/deploy/tearDown.sh
```

If you have deployed multiple instances or you do not have the same `generated` folder that was created when it was
deployed, you can pass the instance ID as an argument to this script.

You can also delete the CloudFormation stacks that were deployed by the CDK directly, as described
in [Deployment with the CDK](deployment/deploy-with-cdk.md#tear-down).
