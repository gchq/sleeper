Building and deploying Sleeper
==============================

This contains instructions on how to deploy Sleeper.

If you just want a local instance for testing, see the documentation
on [deploying to localstack](deployment/deploy-to-localstack.md). This has very limited functionality compared to a
deployed instance.

## Get your environment set up

You will need to get your environment set up correctly so that you can deploy a Sleeper instance to AWS and then
interact with it. See [environment setup](deployment/environment-setup.md) for how to install the Sleeper CLI and create
an environment suitable for deploying Sleeper.

If you set up the [Sleeper CLI deployment environment](deployment/cli-deployment-environment.md), you can connect to it
and build Sleeper like this:

```bash
sleeper environment connect # Get a shell in the EC2 you deployed
sleeper builder             # Get a shell in a builder Docker container (hosted in the EC2)
cd sleeper                  # Change directory to the root of the Git repository
./scripts/build/build.sh
```

If you used the system test deployment described in the getting started guide, you will have already built Sleeper from
the Git repository in a `sleeper builder` container. If you deploy from outside of AWS this will involve lengthy uploads
of build artefacts, which you can avoid with the environment EC2, or your own EC2 instance. If you deploy from your own
EC2, you will need to check out the Git repository inside a `sleeper builder` container yourself.

The `sleeper builder` command gets you a shell inside a Docker container with all the dependencies required to build and
deploy an instance of Sleeper.  The container will be deleted after you exit. You will start in a directory mounted into
the container from a folder in the host home directory under `~/.sleeper`. This workspace will persist after the
container exits, and will be reused by future calls to `sleeper builder`. It also inherits the AWS and Docker
configuration from the host.

## Deployment

Sleeper is deployed using the AWS CDK. You can invoke the CDK to do this either using the automated scripts or by using
the CDK directly.

Either approach should be done from within an EC2 instance set up as described above, to avoid lengthy uploads of large
jar files and Docker images.

### Using the CDK directly

If you prefer to use the CDK CLI directly for deployment, or you want to include Sleeper in your own CDK app, see
[Deployment with the CDK](deployment/deploy-with-cdk.md) for more information.

### Scripted deployment

The scripts for deployment create an instance of Sleeper either from your own configuration files, or from templates.

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

#### Declarative deployment

We have a version of this script that will either create or update an instance, applying your configuration
declaratively:

```bash
./scripts/deploy/deploy.sh <instance-id> <vpc-id> <subnet-ids> ./my-instance/instance.properties
```

This does not currently include Sleeper tables, see issue https://github.com/gchq/sleeper/issues/5870.

#### Update existing instance

The `deployExisting.sh` script can be used to bring an existing instance up to date. This will upload any jars
that have changed, update all the docker images, and perform a `cdk deploy`.

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
