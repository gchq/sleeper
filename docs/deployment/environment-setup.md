Environment setup
=================

## Install Sleeper CLI

The Sleeper CLI contains tools to deploy into AWS, and to build the system. This runs commands inside a Docker
container. This way you can avoid needing to install any dependencies other than Docker on your machine. In the future
we may also publish pre-built artefacts that will make it unnecessary to build Sleeper yourself. When using the Sleeper
CLI, the build and deployment can be invoked from a single script to minimise setup for testing.

### Dependencies

The Sleeper CLI has the following dependencies, please install these first:

* [Bash](https://www.gnu.org/software/bash/): Minimum v3.2. Use `bash --version`.
* [Docker](https://docs.docker.com/get-docker/)

### Install script

You can run the following commands to install the latest version of the CLI from GitHub:

```bash
curl "https://raw.githubusercontent.com/gchq/sleeper/develop/scripts/cli/install.sh" -o ./sleeper-install.sh
chmod +x ./sleeper-install.sh
./sleeper-install.sh
```

The CLI consists of a `sleeper` command with sub-commands. You can use `sleeper aws` or `sleeper cdk` to run `aws` or
`cdk` commands without needing to install the AWS or CDK CLI on your machine. If you set AWS environment variables or
configuration on the host machine, that will be propagated to the Docker container when you use `sleeper`.

You can upgrade to the latest version of the CLI using `sleeper cli upgrade`. This should be done regularly to keep the
build and deployment tools up to date.

## Preparing AWS

### Configure AWS CLI

When you configure the AWS CLI on your machine, this will be passed on to any Sleeper CLI commands. If you
use `sleeper aws configure` this will also be applied outside of the Sleeper CLI, and for other CLI commands.

Here's an example configuration that should allow the SDKs, the CLI and CDK to all access AWS when you set your own
access credentials and profile name:

~/.aws/credentials:

```ini
[named-profile-123456789]
aws_access_key_id = abcd12345
aws_secret_access_key = defg12345
aws_session_token = hijK12345
```

~/.aws/config

```ini
[profile named-profile-123456789]
region = eu-west-2
```

~/.bashrc:

```bash
export AWS_PROFILE=named-profile-123456789
export AWS_REGION=eu-west-2
```

Also see
the [AWS IAM guide for CLI access](https://docs.aws.amazon.com/singlesignon/latest/userguide/howtogetcredentials.html).

### Bootstrapping CDK

To deploy Sleeper into your AWS account you will need to have bootstrapped CDK in the
account. Bootstrapping installs all the resources that CDK needs to do deployments. Note
that bootstrapping CDK is a one-time action for the account that is nothing to do with
Sleeper itself. See
[this link](https://docs.aws.amazon.com/cdk/latest/guide/bootstrapping.html) for guidance
on how to bootstrap CDK in your account. Note that the `cdk bootstrap` command should
not be run from inside the sleeper directory. You can run `cdk bootstrap` in a Sleeper CLI
Docker container, with `sleeper cdk bootstrap`.

### Lambda Reserved Concurrency

When deploying Sleeper, depending on the stacks you need, it will deploy a few Lambda
functions into your account. Some of these Lambda functions are configured to run
with reserved concurrency of 1. In order to allow this you will need to make
sure you have enough free reserved concurrency in your account.

You will need a reserved account concurrency of at most 6 for all the Sleeper stacks
to be deployed. In order to check how many you have, go to the Lambda section in your
AWS Console and check the dashboard. It should say at the top "full account concurrency = X"
(usually 1000) and "unreserved account concurrency = Y". You can't use the last 100 of your
limit. So if Y is greater than or equal to X-100 you won't be able to deploy Sleeper
and you will have to see if you can adjust your existing lambdas to free some up.

## Deployment environment

To deploy Sleeper, you'll need a VPC that meets certain requirements. You'll also want an EC2 instance to deploy from,
to avoid lengthy uploads of large jar files and Docker images from outside AWS. You can use the Sleeper CLI to create
both of these, see the documentation for the [Sleeper CLI deployment environment](cli-deployment-environment.md).

If you prefer to use your own VPC, you'll need to ensure it meets Sleeper's requirements. It should ideally have
multiple private subnets in different availability zones. Those subnets should have egress, e.g. via a NAT gateway. The
VPC should have gateway endpoints for S3 and DynamoDB. If there is no gateway endpoint for S3, deployment of a Sleeper
instance will fail in the CDK. Note that Sleeper will not use the default security group of the VPC.

If you prefer to use your own EC2, it should run on an x86_64 architecture, with Bash and Docker, and have enough
resources to build code for Maven and Rust. We've tested with 8GB RAM and 2 vCPUs, with `t3.large`. We recommend 4 vCPUs
(`t3.xlarge`), as that takes the build from over 40 minutes with 2 vCPUs, to around 20 minutes for the first build.

The [Sleeper CLI deployment environment](cli-deployment-environment.md) includes options to deploy an EC2 to
an existing VPC, or a VPC on its own.

Once you've got a suitable VPC, and an EC2 with the Sleeper CLI installed, you can either move on
to the [deployment guide](../deployment-guide.md), or the [getting started guide deployment section](../getting-started.md#deployment)
to use a testing setup.

You're now ready to build and deploy Sleeper.
