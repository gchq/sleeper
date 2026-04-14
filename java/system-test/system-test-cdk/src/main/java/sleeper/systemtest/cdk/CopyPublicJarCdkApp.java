/*
 * Copyright 2022-2026 Crown Copyright
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
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefactRepositories;
import sleeper.cdk.artefacts.jars.CopyJarProvider;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.artefacts.jars.SleeperJarsFromFileSystem;
import sleeper.cdk.util.CdkContext;

import java.nio.file.Path;

/**
 * An app to test copying a jar file from a public repository to an S3 bucket with a CDK custom resource.
 */
public class CopyPublicJarCdkApp extends Stack {

    public CopyPublicJarCdkApp(Construct scope, String id, StackProps stackProps, Props props) {
        super(scope, id, stackProps);

        IBucket bucket = SleeperArtefactRepositories.createJarsBucket(this, props.deploymentId());
        CopyJarProvider copyJarProvider = CopyJarProvider.Builder.create(this, "CopyJars")
                .jars(props.jars())
                .functionName("sleeper-" + props.deploymentId() + "-copy-jar")
                .functionLogGroup(LogGroup.Builder.create(this, "LambdaLogGroup")
                        .logGroupName("sleeper-" + props.deploymentId() + "-copy-jar")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.RETAIN)
                        .build())
                .providerLogGroup(LogGroup.Builder.create(this, "ProviderLogGroup")
                        .logGroupName("sleeper-" + props.deploymentId() + "-copy-jar-provider")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.RETAIN)
                        .build())
                .build();
        CustomResource copyJar = copyJarProvider.createCopyJar(this, "CopyJar", props.sourceUrl(), bucket.getBucketName(), "test.jar");
        CfnOutput.Builder.create(this, "VersionId")
                .value(copyJar.getAttString("versionId"))
                .build();
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        Props props = Props.from(CdkContext.from(app));
        new CopyPublicJarCdkApp(app, props.deploymentId(),
                StackProps.builder().env(environment).build(), props);
        app.synth();
    }

    public record Props(String deploymentId, String sourceUrl, SleeperJars jars) {

        public static Props from(CdkContext context) {
            return new Props(
                    context.tryGetContext("id"),
                    context.tryGetContext("sourceUrl"),
                    SleeperJarsFromFileSystem.fromJarsDirectory(
                            Path.of(context.tryGetContext("jarsDirectory"))));
        }
    }

}
