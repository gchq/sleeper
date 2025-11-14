Deployment with the CDK
=======================

Sleeper is deployed with the AWS Cloud Development Kit (CDK). This can be done either with scripts as described in
the [deployment guide](../deployment-guide.md#scripted-deployment), or by using the CDK directly. This document covers
deployment using the CDK CLI directly.

### Uploading artefacts to AWS

Details of Docker images to be uploaded can be found [here](/docs/deployment/images-to-upload.md).

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
                .deployPaused(false)
                .build());
```
