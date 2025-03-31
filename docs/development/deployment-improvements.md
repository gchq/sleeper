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

Currently these scripts perform some preparatory steps before they invoke the CDK. We'll look at them one at a time,
then see how this is handled in the CDK.

We'll also look at other potential improvements, including to creation and configuration of Sleeper tables.

## Build the system

This script must be run before any of the other scripts can be used. This builds the code, and outputs artifacts to the
`scripts/jars` and `scripts/docker` directories. This is currently the only supported way to prepare to run the scripts
locally.

When we add support to deploy a published version of Sleeper in https://github.com/gchq/sleeper/issues/1330, we will
add an alternative for this, in some combination of:

- Pull artifacts locally from a published location at installation
- Pull artifacts in AWS from a configured location as part of the CDK deployment

One option for the CLI would be to retrieve a jar or executable for the client when you install the CLI, instead of a
Docker image. This could be used as the entry point for all operations that are currently Bash scripts, as described
in https://github.com/gchq/sleeper/issues/1328.

## Deploy new instance

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

We would ideally make this script unnecessary. The functionality in this script could be included in the CDK app, so
that you could invoke the CDK directly. When we add support to deploy a published version of Sleeper
in https://github.com/gchq/sleeper/issues/1330, we would like to move the upload of jars and Docker images into the CDK
app. When we add support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, there are some
potential changes to how the instance configuration is managed here, including:

- We could only generate values for pre-populated instance properties if they are unset
- We could adjust the templates so that they match the structure of an instance configuration you would set yourself,
  as in https://github.com/gchq/sleeper/issues/3629
- We could remove the templates and force you to create your own configuration
