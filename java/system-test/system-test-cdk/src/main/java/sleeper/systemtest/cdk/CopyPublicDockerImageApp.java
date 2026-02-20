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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.LifecycleRule;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecr.TagStatus;
import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.artefacts.containers.CopyContainerImageStack;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.artefacts.jars.SleeperJarsFromFileSystem;
import sleeper.cdk.util.CdkContext;

import java.nio.file.Path;
import java.util.List;

/**
 * An app to test copying a Docker image from a public repository into ECR with a CDK custom resource.
 */
public class CopyPublicDockerImageApp extends Stack {

    public CopyPublicDockerImageApp(Construct scope, String id, StackProps stackProps, Props props) {
        IRepository repository = Repository.Builder.create(this, "Repository")
                .repositoryName(props.deploymentId() + "/test")
                .removalPolicy(RemovalPolicy.DESTROY)
                .lifecycleRules(List.of(
                        LifecycleRule.builder()
                                .description("Delete untagged images")
                                .tagStatus(TagStatus.UNTAGGED)
                                .maxImageAge(Duration.days(1))
                                .rulePriority(1)
                                .build(),
                        LifecycleRule.builder()
                                .description("Keep images for 365 days")
                                .tagStatus(TagStatus.ANY)
                                .maxImageAge(Duration.days(365))
                                .rulePriority(2)
                                .build()))
                .emptyOnDelete(true)
                .build();
        new CopyContainerImageStack(this, "CopyContainer", props.deploymentId(), props.jars())
                .createCopyContainerImage(this, "CopyImage", props.sourceImageName(), repository.getRepositoryUri());
    }

    public record Props(String deploymentId, String sourceImageName, SleeperJars jars) {

        public static Props from(S3Client s3Client, CdkContext context) {
            return new Props(
                    context.tryGetContext("id"),
                    context.tryGetContext("sourceImageName"),
                    SleeperJarsFromFileSystem.fromJarsDirectory(
                            Path.of(context.tryGetContext("jarsDirectory"))));
        }
    }

}
