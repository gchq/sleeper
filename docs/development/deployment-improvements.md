Potential deployment improvements
=================================

This is an overview of the current deployment process of Sleeper and how it relates to planned and potential
improvements.

We have the following epics for improvements to deployment:
- https://github.com/gchq/sleeper/issues/3693 Declarative deployment for infrastructure as code
- https://github.com/gchq/sleeper/issues/4235 Graceful upgrade of a Sleeper instance

At time of writing the deployment process consists of:
- The CDK app which deploys Sleeper
- A script to build the system at `scripts/build/build.sh`
- Scripts to publish and retrieve published artefacts, see [publishing artefacts](./publishing.md)
- Scripts to deploy an instance, see the [deployment guide](../deployment-guide.md)
- The admin client to edit the instance configuration at `scripts/utility/adminClient.sh`

We'll look at each of these one at a time, and see how they relate to the CDK app.

We'll also look at other potential improvements, including to creation and configuration of Sleeper tables.

## Build the system (`scripts/build/build.sh`)

This script must be run before any of the other scripts can be used. This builds the code, and outputs artefacts to the
`scripts/jars` and `scripts/docker` directories.

This is one of two supported ways to prepare to run the other scripts locally, alongside published artefacts described
below.

## Published artefacts

See [publishing artefacts](./publishing.md).

We can use `scripts/dev/publishMaven.sh` and `scripts/dev/publishDocker.sh` to publish artefacts to external
repositories.

We can use `scripts/deploy/installJarsFromMaven.sh` to retrieve the jars locally from Maven,
and `scripts/deploy/setDeployFromRemoteDocker.sh` to configure our scripts to retrieve Docker images from a remote
repository during deployment.

### Potential changes

That installation process brings more artefacts locally than we usually need to. For most scripts, we just use the
clients jar, and for deployment we just use the CDK jar. We also don't necessarily need to pull the Docker images
locally during deployment, as that could potentially be done in CloudFormation.

This relates to plans for the CLI, as we consider a unified CLI to replace the scripts. We may adjust the entrypoint for
operations currently implemented as scripts, in https://github.com/gchq/sleeper/issues/1328. To reduce the number of
jars we have to install, we could set this up so that artefacts are retrieved only when needed, or include everything in
one jar.

## Instance deployment

See the [deployment guide](../deployment-guide.md). We have support for deploying Sleeper either with scripts, or with
the CDK directly, either with your own CDK app or with ours in the CDK CLI.

Our scripts take an instance ID, VPC ID, subnet IDs, and optional parameters to point it to an instance configuration
and whether to deploy the instance paused.

The `scripts/deploy/deployNew.sh` script can run without an instance configuration, in which case it builds a
configuration from the `scripts/templates` folder. This sets most properties explicitly to their default values, which
may or may not be desirable, as this way we can't expect values to change when we change the defaults.

Next, the S3 jars bucket and ECR repositories for Docker images are created as their own CDK app. This is called the
artefacts deployment, and it has a deployment ID which is the same as the Sleeper instance ID by default. This allows
for tear down of the artefacts, and when it is deleted all artefacts held there will also be deleted.

Next, the jars for deployment of lambdas are read from a local directory under `scripts/jars`, and written to the jars
bucket. The jars bucket name is read from the instance properties, and defaults to a name derived from the artefacts
deployment ID.

Docker is then run against any images that need to be deployed based on the specified optional stacks in the instance
property `sleeper.optional.stacks`. These images are either built from a local directory under `scripts/docker`, or
pulled from a remote repository configured during installation of published artefacts. The images are uploaded to the
ECR repositories in an artefacts deployment. The name of each repository is derived from a prefix, which is set in the
instance properties, and defaults to a name derived from the artefacts deployment ID.

Finally, the instance configuration is written to a local directory and the CDK is invoked pointing to that local
directory to configure the instance. The CDK deploys the jars and Docker images from the jars bucket and ECR
repositories in the artefacts deployment.

Some of the scripts also create Sleeper tables. This is done outside the CDK.

### Potential changes

Currently the process of uploading artefacts for deployment is done as a separate step between two invocations of the
CDK, rather than being done by the CDK in AWS during deployment. Ideally we would remove the necessity for this extra
step.

In order for the retrieval to happen during CDK deployment, we would need a component to do this deployed inside AWS.
That component might then need to support authentication with your private Docker and Maven repositories. We don't have
a way to do that, but we could add some code that assumes unauthenticated repositories, and you could potentially
restrict access some other way.

We want to adjust our CDK code to make it as easy as possible for you to pass the artefacts in yourself.

When we add support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, there are some potential
changes to how the instance configuration is managed here, including:

- We could adjust the templates so that they match the structure of an instance configuration you would set yourself,
  as in https://github.com/gchq/sleeper/issues/3629
- We could remove the templates and force you to create your own configuration

## Tear down (`scripts/deploy/tearDown.sh`)

This script takes an instance ID, shuts down a Sleeper instance and invokes CloudFormation to remove the deployment, as
would be done by the CDK.

There are now no extra operations, it just calls CloudFormation to delete the stacks, and waits for this to complete.
You can achieve the same thing by using the CDK or CloudFormation directly.

## Admin client (`scripts/utility/adminClient.sh`)

This script invokes a CLI to perform various admin operations. The relevant parts here are to edit the configuration of
a Sleeper instance and its tables. This opens a text editor for the instance properties or table properties, and then
saves any changes directly to AWS. If you edit an instance property that requires a CDK deployment to apply, the
instance configuration is written to a local directory and the CDK is invoked pointing to that configuration.

### Potential changes

We've recently improved support for declarative deployment, but there are futher improvments we can make,
in https://github.com/gchq/sleeper/issues/3693. At some point the admin client may no longer be the recommended way to
apply changes to instance or table properties. Instead, you would check your instance configuration into version
control, and when you make a change you would invoke the CDK to update the Sleeper instance.

We may add a tool to let you apply changes to properties that would only invoke the CDK if you've changed a property
that requires a CDK deployment in order to change it. Alternatively we may recommend putting the properties fully under
the control of the CDK.

## Sleeper table configuration

Currently Sleeper tables are managed by a separate set of scripts to manage Sleeper tables, documented
separately [here](../usage/tables.md).

When we add support for declarative deployment in https://github.com/gchq/sleeper/issues/3693, we may add an alternative
flow for applying configuration of Sleeper tables. We may split that out as a separate epic.

Here are some potential improvements currently under that epic:
- https://github.com/gchq/sleeper/issues/5870 Support configuring Sleeper tables as part of a CDK invocation
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
