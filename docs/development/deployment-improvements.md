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

## Build the system

This script must be run before any of the other scripts can be used. This builds the code, and outputs artifacts to the
`scripts/jars` and `scripts/docker` directories. This is currently the only supported way to prepare to run the scripts
locally.

When we add support to deploy a published version of Sleeper, we will do some combination of retrieving these artifacts
from a published location, and/or retrieve them separately as part of the CDK deployment.

One option for the CLI would be to retrieve a jar or executable for the client when you install the CLI, instead of a
Docker image. This could be used as the entry point for all operations that are currently Bash scripts.

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
