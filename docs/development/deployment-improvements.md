Potential deployment improvements
=================================

This is an overview of the current deployment process of Sleeper and how it relates to planned and potential
improvements.

We have the following epics for improvements to deployment:
- https://github.com/gchq/sleeper/issues/1330 Support deploying a published version of Sleeper.
- https://github.com/gchq/sleeper/issues/3693 Declarative deployment for infrastructure as code.

At time of writing the deployment process consists of:
- The CDK app which deploys Sleeper
- A script to build the system at `scripts/build/build.sh`
- A script to deploy a new instance at `scripts/deploy/deployNew.sh`
- A script to deploy an existing instance at `scripts/deploy/deployExisting.sh`
- A script to tear down an instance at `scripts/deploy/tearDown.sh`
- The admin client to edit the instance configuration at `scripts/utility/adminClient.sh`

Currently these scripts perform some preparatory steps before they invoke the CDK. We'll look at them one at a time,
then see how this is handled in the CDK.

We'll also look at other potential improvements, including to creation and configuration of Sleeper tables.

## Build the system (`scripts/build/build.sh`)

This script must be run before any of the other scripts can be used. This builds the code, and outputs artifacts to the
`scripts/jars` and `scripts/docker` directories.

This is currently the only supported way to prepare to run the other scripts locally.

### Potential changes

When we add support to deploy a published version of Sleeper in https://github.com/gchq/sleeper/issues/1330, we will
need an alternative for this, in some combination of:

- Pull artifacts locally from a published location at installation
- Pull artifacts in AWS from a configured location as part of the CDK deployment

We will need to decide which published locations we should support for the jars and Docker images, potentially
including:

- A jars bucket in another AWS account
- A shared jars bucket for instances in the same AWS account (as in the current automated system tests)
- A Maven repository, either specified or e.g. Maven central, GitHub packages
- Specified Docker repositories, or e.g. Docker Hub, GitHub packages

We could support setting specific locations for every individual jar and Docker image, or just with predefined names
under a given location.

We could provide support to publish artifacts to each of these locations as part of the build process.

This also relates to plans for the CLI, as we consider a unified CLI to replace the scripts. Other scripts currently
require jars built by this script. We may adjust the entrypoint for operations currently implemented as scripts,
in https://github.com/gchq/sleeper/issues/1328.

## Deploy new instance (`scripts/deploy/deployNew.sh`)

This script takes an instance ID, VPC ID, subnet IDs, and optional parameters to point it to an instance configuration
and whether to deploy the instance paused.

If you don't point it to an instance configuration, it builds a configuration from the `scripts/templates` folder.
In either case, it populates the configuration with some additional settings:

- The instance ID, both in its own property and an instance tag
- Generated names for the jars bucket and various ECR Docker repositories (which do not yet exist)
- Properties for the AWS account ID, region, VPC ID, subnet IDs

Next, the jars for deployment of lambdas are read from a local directory under `scripts/jars`, and written to an S3
jars bucket which is created if it does not exist. The name of the jars bucket is generated from the instance ID, and
cannot be set even if you specify it in your own instance configuration.

Docker is then run against any images that need to be deployed based on the specified optional stacks in the instance
property `sleeper.optional.stacks`. These images are built from a local directory under `scripts/docker`. For each
image, an ECR repository is created in AWS, and the image is pushed to that repository. The name of each repository is
generated from the instance ID, and cannot be set even if you specify it in your own instance configuration.

Finally, the instance configuration is written to a local directory and the CDK is invoked pointing to that local
directory to configure the instance. The CDK deploys the jars and Docker images from the jars bucket and ECR
repositories that were created. The jars bucket and ECR repositories are never managed by the CDK, and must be deleted
separately during tear down.

This script also currently creates Sleeper tables. There is one table in the templates, and any tables specified in your
own instance configuration will be added.

### Potential changes

When we add support to deploy a published version of Sleeper in https://github.com/gchq/sleeper/issues/1330, we would
like to move the upload of jars and Docker images into the CDK app. When we add support for declarative deployment
in https://github.com/gchq/sleeper/issues/3693, there are some potential changes to how the instance configuration is
managed here, including:

- We could only generate values for pre-populated instance properties if they are unset
- We could adjust the templates so that they match the structure of an instance configuration you would set yourself,
  as in https://github.com/gchq/sleeper/issues/3629
- We could remove the templates and force you to create your own configuration

These changes may make this script unnecessary, or it could call the CDK directly. Operations for Sleeper tables could
be handled separately, or this script could combine a CDK invocation with management of Sleeper table configuration.

## Deploy existing instance (`scripts/deploy/deployExisting.sh`)

This script only takes an instance ID and an optional parameter for whether the instance should be paused. This can
be used to update an existing instance.

This reads the instance configuration from AWS and writes it to a local directory for the CDK. It updates the jars
in the jars bucket from the local directory `scripts/jars`, and builds and pushes Docker images to ECR from the local
directory `scripts/docker`, based on the specified optional stacks in the instance property `sleeper.optional.stacks`.
This checks the current state of the jars and Docker images in AWS. Currently if Docker images already exist in ECR with
the same Sleeper version number being deployed, the Docker images are not rebuilt or redeployed.

Once the jars and Docker images are updated in AWS, the CDK is invoked with the same instance configuration that was
read from AWS.

### Potential changes

When we add support to deploy a published version of Sleeper in https://github.com/gchq/sleeper/issues/1330,
if we move the upload of jars and Docker images into the CDK app, all functionality in this script may be moved into the
CDK. If we do that first, we may not need any additional changes for declarative deployment
in https://github.com/gchq/sleeper/issues/3693, as invoking the CDK directly should accomplish that goal.
This would be done in https://github.com/gchq/sleeper/issues/3628.

We could remove this script, or have it call the CDK directly. Operations for Sleeper tables could be handled
separately, or this script could combine a CDK invocation with management of Sleeper table configuration.

## Tear down (`scripts/deploy/tearDown.sh`)

This script takes an instance ID, shuts down a Sleeper instance and invokes CloudFormation to remove the deployment, as
would be done by the CDK.

Before invoking the CDK, this performs several operations to shut down the instance:
- Pause all scheduled rules for background operations
- Stop all long running processes, e.g. ECS tasks, EMR clusters and applications

This then waits for all CloudFormation stacks to delete, and performs some extra operations afterwards:
- Delete the jars S3 bucket and ECR Docker repositories
- Clear the local directory used to pass an instance configuration to the CDK

### Potential changes

When we add support to deploy a published version of Sleeper in https://github.com/gchq/sleeper/issues/1330, if we move
the upload of jars and Docker images into the CDK app, it should no longer be necessary to delete the jars bucket and
ECR repositories here.

The fact that the shut down operations are done here poses an additional problem when adding/removing optional stacks
from a Sleeper instance, as this shut down is not done when an optional stack is removed. We would like to move the
instance shut down operations into the CDK as part of each stack, in https://github.com/gchq/sleeper/issues/4527. This
is currently part of the epic for support for declarative deployment, in https://github.com/gchq/sleeper/issues/3693.

When these changes are done, this script should be unnecessary, or can call the CDK directly. All functionality will be
in the CDK app.

## Admin client (`scripts/utility/adminClient.sh`)

This script invokes a CLI to perform various admin operations. The relevant parts here are to edit the configuration of
a Sleeper instance and its tables. This opens a text editor for the instance properties or table properties, and then
saves any changes directly to AWS. If you edit an instance property that requires a CDK deployment to apply, the
instance configuration is written to a local directory and the CDK is invoked pointing to that configuration.

When we add support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, this may no longer be the
recommended way to apply changes to instance or table properties. Instead, you would check your instance configuration
into version control, and when you make a change you would invoke the CDK to update the Sleeper instance. This may
include table properties as well.

## Sleeper table configuration

Currently Sleeper tables are managed by a separate set of scripts to manage Sleeper tables, documented
separately [here](../usage/tables.md).

When we add support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, we may add an alternative
flow for applying configuration of Sleeper tables. We may split that out as a separate epic.

Here are some potential improvements currently under that epic:
- https://github.com/gchq/sleeper/issues/583 Add table split points into local configuration folder structure
- https://github.com/gchq/sleeper/issues/3627 Declarative update of table configuration

## Other improvements

One option for the CLI would be to retrieve a jar or executable for the client when you install the CLI, instead of a
Docker image. This could be used as the entry point for all operations that are currently Bash scripts, as described
in https://github.com/gchq/sleeper/issues/1328. This could potentially simplify the local build artifacts currently
required for scripts to work with an instance of Sleeper.

As part of support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, we have an issue to apply
this to manual system tests, so we can have different configurations we can deploy for testing,
in https://github.com/gchq/sleeper/issues/2384.

We also have a separate epic https://github.com/gchq/sleeper/issues/4235 for graceful upgrade of a Sleeper instance
while it's running. This is not currently supported, but we can add appropriate testing so we can support this in the
future.
