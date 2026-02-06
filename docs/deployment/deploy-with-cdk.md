Deployment with the CDK
=======================

Sleeper is deployed with the AWS Cloud Development Kit (CDK). This can be done either with scripts as described in
the [deployment guide](../deployment-guide.md#scripted-deployment), or by using the CDK directly. This document covers
deployment using the CDK CLI directly.

### Uploading artefacts to AWS

Some jars and Docker images must be uploaded to AWS before you can deploy an instance of Sleeper. We have a CDK app
`SleeperArtefactsCdkApp` which creates an S3 bucket and ECR repositories to hold these artefacts, but does not
upload the artefacts. You can also include this in your own CDK app with `SleeperArtefacts`. You can use our tools to
upload the artefacts as a separate step, or implement your own way to do this that may be specific to your Maven and
Docker repositories.

The scripted deployment uploads the jars from the local `scripts/jars` directory within the Git repository. The Docker
images are either built from the local `scripts/docker` directory or pulled from a remote repository if that is
configured. You could replicate that behaviour yourself with the script `scripts/deploy/uploadArtefacts.sh`, or use our
Java classes `SyncJars` and `UploadDockerImagesToEcr`, or implement your own way to upload these artefacts.

As part of `scripts/build/build.sh`, the jars are built and output to `scripts/jars`, and the Docker builds are prepared
in separate directories for each Docker image under `scripts/docker`. You can also use
our [publishing tools](../development/publishing.md) to prepare the artefacts.

It's important to upload artefacts from within AWS to avoid lengthy uploads into AWS. Usually this is done from an EC2
instance.

#### `uploadArtefacts.sh`

This script can upload artefacts to an existing CDK deployment. You can either pass in the deployment ID that you used
for the CDK deployment, or pass in an instance properties file for an instance that is configured to use that artefacts
deployment. In the latter case, Docker images will only be uploaded if they are required with your instance
configuration. Run `uploadArtefacts.sh --help` for details.

If your artefacts deployment ID is different from the instance ID, you can set the deployment ID in the instance
property [`sleeper.artefacts.deployment`](../usage/properties/instance/user/common.md). If you prefer to avoid setting
the instance ID or deployment ID in your properties file, you can pass arguments for both the path to the properties
file, and the deployment ID.

Here's an example with a CDK command to create an artefacts deployment, and a call to the script to upload all artefacts
to that deployment:

```bash
DEPLOYMENT_ID=my-deployment
cdk deploy --all -c id=$DEPLOYMENT_ID -a "java -cp ./scripts/jars/cdk-<version>.jar sleeper.cdk.SleeperArtefactsCdkApp"
./scripts/deploy/uploadArtefacts.sh --id $DEPLOYMENT_ID
```

#### Direct upload

If you prefer to implement this yourself, details of Docker images to be uploaded can be
found [here](/docs/deployment/docker-images.md). That document includes details of how to build and push the images to
ECR, as it is done by the automated scripts.

You'll also need to create an S3 bucket for jars, and upload the contents of the `scripts/jars` directory to it. That
directory is created during a build, or during installation of a published version. The jars S3 bucket needs to have
versioning enabled so we can tie a CDK deployment to specific versions of each jar.

When not using an artefacts CDK deployment, you can set the instance properties `sleeper.jars.bucket`
and `sleeper.ecr.repository.prefix` instead of `sleeper.artefacts.deployment`.

### Including Sleeper in your CDK app

Sleeper supports deployment as part of your own CDK app, either as its own stack or as a nested stack under your stack.
If you have published Sleeper to a Maven repository as described in the [publishing guide](../development/publishing.md)
you can add the Sleeper CDK module as a Maven artefact like this:

```xml
<dependency>
    <groupId>sleeper</groupId>
    <artifactId>cdk</artifactId>
    <version>version.number.here</version>
</dependency>
```

Use the class `SleeperInstance` to add instances of Sleeper to your app. To load instance and table properties from
the local file system you can use `DeployInstanceConfiguration.fromLocalConfiguration`. Here's an example:

```java
Stack stack = Stack.Builder.create(app, "MyStack")
        .stackName("my-stack")
        .env(environment)
        .build();
SleeperInstanceConfiguration myInstanceConfig = SleeperInstanceConfiguration.fromLocalConfiguration(
        workingDir.resolve("my-instance/instance.properties"));
SleeperInstance.createAsNestedStack(stack, "MyInstance",
        NestedStackProps.builder()
                .description("My instance")
                .build(),
        SleeperInstanceProps.builder(myInstanceConfig, s3Client, dynamoClient)
                .networking(new SleeperNetworking(vpc, vpc.getPrivateSubnets()))
                .deployPaused(false)
                .build());
```

### Using the CDK CLI

To deploy a Sleeper instance to AWS with the CDK, you need an [instance configuration](instance-configuration.md) and
a [suitable environment](environment-setup.md). The artefacts will need to be uploaded as described in the section
above. You can either use the instance ID as the deployment ID for the artefacts, or you can set the deployment ID in
the CDK context variable `artefactsId`, or the instance property `sleeper.artefacts.deployment`.

You can use the same CDK apps used by the automated scripts, or your own CDK configuration. We'll give examples with the
CDK apps used by the automated scripts. The following commands will deploy a Sleeper instance:

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
INSTANCE_ID=my-instance-id
VPC_ID=my-vpc-id
SUBNETS=my-subnet-1,my-subnet-2,my-subnet-3
SCRIPTS_DIR=./scripts # This is from the root of the Sleeper Git repository
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
cdk deploy --all --app "java -cp $SCRIPTS_DIR/jars/cdk-$VERSION.jar sleeper.cdk.SleeperArtefactsCdkApp" -c id=$INSTANCE_ID
"$SCRIPTS_DIR/deploy/uploadArtefacts.sh" --id $INSTANCE_ID --properties $INSTANCE_PROPERTIES
cdk deploy --all -a "java -cp $SCRIPTS_DIR/jars/cdk-$VERSION.jar sleeper.cdk.SleeperCdkApp" \
    -c id=$INSTANCE_ID -c propertiesfile=$INSTANCE_PROPERTIES \
    -c vpc=$VPC_ID -c subnets=$SUBNETS -c newinstance=true
```

To avoid having to explicitly give approval for deploying all the stacks, you can add "--require-approval never" to the
command.

If you'd like to include data generation for system tests, use the system test CDK app instead.

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
INSTANCE_ID=my-instance-id
VPC_ID=my-vpc-id
SUBNETS=my-subnet-1,my-subnet-2,my-subnet-3
SCRIPTS_DIR=./scripts # This is from the root of the Sleeper Git repository
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")
cdk deploy --all --app "java -cp $SCRIPTS_DIR/jars/cdk-$VERSION.jar sleeper.cdk.SleeperArtefactsCdkApp" \
    -c id=$INSTANCE_ID -c extraEcrImages=system-test
"$SCRIPTS_DIR/deploy/uploadArtefacts.sh" --id $INSTANCE_ID --properties $INSTANCE_PROPERTIES --extra-images system-test
cdk deploy --all -a "java -cp $SCRIPTS_DIR/jars/system-test-$VERSION-utility.jar sleeper.systemtest.cdk.SystemTestApp" \
    -c id=$INSTANCE_ID -c propertiesfile=$INSTANCE_PROPERTIES \
    -c vpc=$VPC_ID -c subnets=$SUBNETS -c newinstance=true
```

#### Tear down

If the artefacts and the Sleeper instance are each deployed in their own CDK app, with `SleeperArtefactsCdkApp` and
`SleeperCdkApp`, you can tear down an instance of Sleeper either by deleting the CloudFormation stacks, or with the CDK
CLI. You may need to delete the Sleeper instance before deleting the artefacts used to deploy it. Here's an example:

```bash
INSTANCE_PROPERTIES=/path/to/instance.properties
INSTANCE_ID=my-instance-id
SCRIPTS_DIR=./scripts # From the root of the Sleeper Git repository
VERSION=$(cat "$SCRIPTS_DIR/templates/version.txt")

cdk destroy --all -c id=$INSTANCE_ID -c propertiesfile=$INSTANCE_PROPERTIES -c validate=false -a "java -cp $SCRIPTS_DIR/jars/cdk-$VERSION.jar sleeper.cdk.SleeperCdkApp"
cdk destroy --all -c id=$INSTANCE_ID -a "java -cp $SCRIPTS_DIR/jars/cdk-$VERSION.jar sleeper.cdk.SleeperArtefactsCdkApp"
```
