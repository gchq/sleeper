/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.systemtest.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstance;
import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.containers.CopyContainerImageStack;
import sleeper.cdk.artefacts.containers.SleeperContainerImages;
import sleeper.cdk.artefacts.containers.SleeperContainerImagesFromStack;
import sleeper.cdk.artefacts.jars.SleeperJarVersionIdProvider;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.artefacts.jars.SleeperJarsFromProperties;
import sleeper.cdk.util.CdkContext;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;

import java.nio.file.Path;

/**
 * An example app for testing deployment of Sleeper with artefacts from an external repository.
 */
public class SleeperInstanceFromPublicArtefactsApp extends Stack {

    public SleeperInstanceFromPublicArtefactsApp(Construct scope, String id, StackProps stackProps, Props props) {
        CopyContainerImageStack copyContainers = new CopyContainerImageStack(this, "CopyContainer", props.deploymentId(), props.jars());
        SleeperContainerImages images = new SleeperContainerImagesFromStack(props.instanceProperties(), copyContainers, props.sourceImagePrefix());
        SleeperInstance.createAsNestedStack(this, "Instance", SleeperInstanceProps.builder()
                .instanceProperties(props.instanceProperties())
                .artefacts(SleeperArtefacts.from(props.jars(), images))
                .build());
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        try (S3Client s3Client = S3Client.create()) {
            Props props = Props.from(s3Client, CdkContext.from(app));
            new SleeperInstanceFromPublicArtefactsApp(app, props.deploymentId(),
                    StackProps.builder().env(environment).build(), props);

            app.synth();
        }
    }

    public record Props(String deploymentId, String sourceImagePrefix, InstanceProperties instanceProperties, SleeperJars jars) {

        public static Props from(S3Client s3Client, CdkContext context) {
            InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesNoValidation(
                    Path.of(context.tryGetContext("propertiesfile")));
            SleeperJars jars = new SleeperJarsFromProperties(instanceProperties, SleeperJarVersionIdProvider.from(s3Client, instanceProperties));
            return new Props(
                    context.tryGetContext("id"),
                    context.tryGetContext("sourceImagePrefix"),
                    instanceProperties, jars);
        }
    }
}
