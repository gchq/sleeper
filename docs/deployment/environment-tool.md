Sleeper environment tool
========================

The Sleeper environment tool can create an environment in your AWS account that's suitable for deploying Sleeper.

To deploy Sleeper in AWS, we need a suitable networking context for a Sleeper instance to interact with AWS services.
We want the deployment to happen inside AWS, so that the necessary compiled artefacts don't need to be uploaded from
outside. The environment satisfies these constraints by deploying a VPC and an EC2 instance.

Before you start, ensure you have installed the tools as described in [Sleeper Docker tools](docker-tools.md), and
followed the [Deployment environment setup](environment-setup.md) up to the creation of the deployment environment.

### Authentication

To use the tool against AWS, you need to authenticate against your AWS account. The AWS configuration will be mounted
into Sleeper Docker tool containers from your home directory on your host machine. AWS environment variables will also
be propagated to Sleeper Docker tool containers if you set them on the host machine.

You can configure AWS without installing the AWS CLI by running `sleeper aws configure`, or any other AWS commands
you may need with `sleeper aws`.

Most Sleeper clients also require you to set the default region in your AWS configuration.

### AWS CDK deployment

If the CDK has never been bootstrapped in your AWS account, this must be done first. This only needs to be done
once in a given AWS account. Replace the number below with your AWS account number. See
[the AWS documentation](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping-env.html) for further information.

```bash
sleeper cdk bootstrap aws://123456789012/eu-west-2
```

### Deploy/connect to an environment

The Sleeper environment tool can create a machine in AWS to deploy Sleeper from (an EC2 instance) and a networking
context that is suitable for deploying Sleeper. The machine in AWS avoids the need for lengthy uploads of build
artefacts from outside AWS.

A VPC is deployed with endpoints to access AWS services, and a default security group that will deny all incoming and
outgoing traffic. Sleeper is configured to not use the default security group for the VPC, and instead uses custom
defined ones when relevant.

This tool will automatically configure authentication such that once you're in the EC2 instance you'll have
administrator access to your AWS account. This is intended for development and testing. A production instance of Sleeper
is likely to need some extra security setup, and you may wish to avoid deploying an EC2 with admin access to a
production AWS account.

You can deploy a fresh environment like this:

```bash
# Replace <environment-id> with your own unique environment ID.
sleeper environment deploy <environment-id>
```

Parameters after the environment ID will be passed to a `cdk deploy --all` command. By default this will deploy a fresh
VPC and EC2, either of which may be omitted:

```bash
# Only deploy VPC (running this with an existing environment will remove the EC2)
sleeper environment deploy <environment-id> -c deployEc2=false

# Deploy EC2 in an existing VPC
sleeper environment deploy <environment-id> -c vpcId=[vpc-id]
```

If someone else has already created an environment that you want to share, you can add it as long as you have access
and the EC2 is currently running. You can create your own user on the EC2, but there's no authorisation that links your
identity to a particular user. Anyone with access to the EC2 can connect as any user. Users will have separate instances
of the Sleeper repository checked out with Git.

```bash
sleeper environment add <environment-id>
# Make sure your username is different from other users.
# If you already have a user on the EC2, you can use setuser instead of adduser.
sleeper environment adduser <username>
```

The `sleeper environment deploy` command will wait for the EC2 instance to be deployed. The following command will
SSH into the EC2 with EC2 Instance Connect and SSM Session Manager:

```bash
sleeper environment connect
```

#### Cloud Init

Immediately after it's deployed, commands will run on the EC2 instance to install the Sleeper CLI. Once you're
connected, you can check the progress of those commands like this:

```bash
cloud-init status
```

You can check the output like this (add `-f` if you'd like to follow the progress):

```bash
tail /var/log/cloud-init-output.log
```

The cloud-init process will install the Sleeper CLI and clone the Sleeper Git repository. Once it has finished the EC2
will restart. If you added an existing environment, adding a user will clone your own copy of the repository, but
cloud-init will already have been done, and the EC2 will not restart.

### Using the environment EC2

Run `sleeper builder` in the EC2 to start a builder Docker container with the Git repository mounted into it:

```bash
sleeper environment connect      # Get a shell in the EC2 you deployed
sleeper builder                  # Get a shell in a builder Docker container (hosted in the EC2)
cd sleeper                       # Change directory to the root of the Git repository
git checkout --track origin/main # Check out the latest release version
```

The connection to the EC2 is a standard SSH connection, persisting for the current terminal until you use `exit` to end
the session. By default, `sleeper environment connect` will also start a `screen` session in the EC2. If you lose
connection to the EC2, the `screen` session will persist, and next time you connect you will be where you left off.

When connected to the EC2, if you use Sleeper CLI commands that start Docker containers, these containers will run in
the Docker host that's installed in the EC2. Your `sleeper builder` container there will mount its working directory
from your home directory in the EC2, rather than in your local machine. This will persist the copy of the Git repository
in the EC2 between executions of `sleeper builder`.

Before building Sleeper for deployment, or deploying Sleeper from artefacts built there, please ensure you connect to
the EC2 first via `sleeper environment connect`, to avoid lengthy uploads of jars and Docker images.

### The `sleeper environment` command

If you run `sleeper environment` on its own, you'll get a shell inside a Docker container where you can run `aws`, `cdk`
and Sleeper `environment` commands directly, without prefixing with `sleeper`.

You can use `aws` commands there to set the AWS account, region and authentication. You can also set AWS environment
variables or configuration on the host machine, which will be propagated to the Docker container when you use
`sleeper` commands.

There are also subcommands for `sleeper environment` that allow you to deploy an environment, connect to an EC2 in a
deployed environment, and manage different environments and users of an EC2.

Note that `sleeper environment` commands are not intended to be run from inside the EC2 it deploys. When you connect to
an EC2, this will be in a fresh context that is not aware of environments you have deployed or added. You can still use
it to run `aws` and `cdk` commands, although it may be more convenient to use `sleeper builder` for this.

#### SSH connection options

Whether you deployed or added an environment, you can connect to the deployed EC2 like this when it is running:

```bash
sleeper environment connect
```

This will SSH into the machine with EC2 Instance Connect and SSM Session Manager, and create a Linux `screen` session.
If you do not explicitly exit this session, you will reconnect to the same `screen` session next time you connect to the
EC2. If multiple connections are made to the EC2 as the same user, this will take over the `screen` session and
disconnect the previous connection.

You can replace the `screen` command by adding your own parameters to pass to ssh, like this:

```bash
sleeper environment connect bash
```

#### Managing environments

If you've deployed or added multiple environments, you can switch between them like this:

```bash
sleeper environment list
sleeper environment set <environment-id>
```

You can tear down the deployed environment like this:

```bash
sleeper environment destroy <environment-id>
```

Parameters after the environment ID will be passed to a `cdk destroy` command.

#### Managing users

When you deploy or add an environment, you will connect to the EC2 as the default user for the machine. This may not be
desirable if you want to share the EC2, or if you want to automate system tests to run as that user.

From outside the EC2, you can manage operating system users on the EC2 like this:

```bash
sleeper environment adduser <username>
sleeper environment setuser <username>
sleeper environment deluser <username>
```

When you add a new user or set your user, further invocations of `sleeper environment connect` will connect as that
user.

When you add a new user a fresh instance of the Sleeper Git repository will be checked out for that user, accessible
through `sleeper builder` as that user.

There's no authorisation that links your identity to a particular user. Anyone with access to the EC2 can connect as any
user.
