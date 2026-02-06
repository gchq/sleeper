Sleeper CLI deployment environment
==================================

To deploy Sleeper in AWS, we need a suitable networking context for a Sleeper instance to interact with AWS services.
We need to get the necessary compiled artefacts into AWS to deploy the system. We need the correct permissions and
access to be able to deploy and interact with the system. The Sleeper CLI contains tools to establish an environment
that satisfies these constraints.

We'll look at how to prepare to interact with AWS, and how to create a suitable environment.

By default this will deploy networking and an EC2 instance. The EC2 instance is provided to allow easy deployment of
Sleeper, especially for development. It's a machine that can build Sleeper within AWS, and it avoids lengthy uploads of
built artifacts into AWS, particularly jars and Docker images. In the future we may add support for prebuilt artifacts,
in which case the EC2 will not be needed to deploy Sleeper.

The EC2 is deployed with admin access to your AWS account. A production instance of Sleeper is likely to need some extra
security setup, and you may wish to avoid deploying an EC2 with admin access to a production AWS account.

A VPC is deployed with a default security group that will deny all incoming and outgoing traffic.
Sleeper is configured to not use the default security group for the VPC and instead uses custom defined ones when relevant.

For general administration of an existing Sleeper instance it is not necessary to connect to an environment EC2.

### Authentication

To use the Sleeper CLI against AWS, you need to authenticate against your AWS account. The AWS configuration will be
mounted into Sleeper CLI Docker containers from your home directory on your host machine. AWS environment variables will
also be propagated to Sleeper CLI containers if you set them on the host machine.

You can configure AWS without installing the AWS CLI by running `sleeper aws configure`, or any other AWS commands
you may need with `sleeper aws`.

Most Sleeper clients also require you to set the default region in your AWS configuration.

### AWS CDK deployment

If the CDK has never been bootstrapped in your AWS account, this must be done first. This only needs to be done
once in a given AWS account.

```bash
sleeper cdk bootstrap
```

### Deploy/connect to an environment

The Sleeper CLI can create a machine in AWS to deploy Sleeper from (an EC2 instance) and a networking context that is
suitable for deploying Sleeper. The machine in AWS avoids the need for lengthy uploads of build artefacts from outside
AWS.

This tool will automatically configure authentication such that once you're in the EC2 instance you'll have
administrator access to your AWS account. This is intended for development and testing.

You can deploy a fresh environment like this:

```bash
# Replace <environment-id> with your own unique environment ID.
sleeper environment deploy <environment-id>
```

[See below](#managing-environments) for options for this command. By default this will deploy a fresh VPC and EC2,
either of which may be omitted.

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

The `sleeper environment deploy` command will wait for the EC2 instance to be deployed.
You can then SSH to it with EC2 Instance Connect and SSM Session Manager, using this command:

```bash
sleeper environment connect
```

#### Cloud Init

Immediately after it's deployed, commands will run on this instance to install the Sleeper CLI. Once you're connected,
you can check the progress of those commands like this:

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

#### Managing environments

You can deploy either the VPC or the EC2 independently, or specify an existing VPC to deploy the EC2 to.
You must specify a unique environment ID when deploying an environment. Parameters after the environment ID will be
passed to a `cdk deploy --all` command.

```bash
# Deploy EC2 in a new VPC
sleeper environment deploy <environment-id>

# Only deploy VPC (running this with an existing environment will remove the EC2)
sleeper environment deploy <environment-id> -c deployEc2=false

# Deploy EC2 in an existing VPC
sleeper environment deploy <environment-id> -c vpcId=[vpc-id]

# Deploy with nightly system test automation (set nightlyTestDeployId to your own 2-character value)
sleeper environment deploy <environment-id> -c nightlyTestsEnabled=true -c nightlyTestDeployId=my
```

You can add an environment that was previously deployed like this:

```bash
sleeper environment add <environment-id>
```

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

You can switch environments like this:

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
