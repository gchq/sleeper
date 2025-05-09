Sleeper CLI deployment environment
==================================

To deploy Sleeper in AWS, we need a suitable networking context for a Sleeper instance to interact with AWS services.
We need to get the necessary compiled artefacts into AWS to deploy the system. We need the correct permissions and
access to be able to deploy and interact with the system. The Sleeper CLI contains tools to establish an environment
that satisfies these constraints.

We'll look at how to prepare to interact with AWS, and how to create a suitable environment.

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
administrator access to your AWS account. This is suitable for non-production use, see
the [deployment guide](../deployment-guide.md#deployment-environment) for further details.

You can deploy a fresh environment like this:

```bash
# Replace <environment-id> with your own unique environment ID.
sleeper environment deploy <environment-id>
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
