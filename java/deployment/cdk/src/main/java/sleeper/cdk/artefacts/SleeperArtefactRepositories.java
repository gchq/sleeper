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
package sleeper.cdk.artefacts;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.ecr.LifecycleRule;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecr.TagStatus;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketAccessControl;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.constructs.Construct;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.model.SleeperArtefactsLocation;

import java.util.List;

/**
 * AWS resources that will hold artefacts used to deploy Sleeper. This should be deployed separately before deploying a
 * Sleeper instance. The artefacts need to be uploaded as a separate step after deploying this, but before deploying a
 * Sleeper instance that requires those artefacts.
 */
public class SleeperArtefactRepositories {

    private final Construct scope;
    private final String deploymentId;
    private final List<String> extraEcrImages;

    private SleeperArtefactRepositories(Builder builder) {
        scope = builder.scope;
        deploymentId = builder.deploymentId;
        extraEcrImages = builder.extraEcrImages;
        create();
    }

    private void create() {
        Bucket.Builder.create(scope, "JarsBucket")
                .bucketName(SleeperArtefactsLocation.getDefaultJarsBucketName(deploymentId))
                .encryption(BucketEncryption.S3_MANAGED)
                .accessControl(BucketAccessControl.PRIVATE)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                // We enable versioning so that the CDK is able to update functions when the code changes in the bucket.
                // See the following:
                // https://www.define.run/posts/cdk-not-updating-lambda/
                // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
                // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
                .versioned(true)
                .build();

        for (LambdaJar jar : LambdaJar.all()) {
            createRepository(jar.getImageName());
        }

        for (DockerDeployment deployment : DockerDeployment.all()) {
            Repository repository = createRepository(deployment.getDeploymentName());

            if (deployment.isCreateEmrServerlessPolicy()) {
                repository.addToResourcePolicy(PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .principals(List.of(new ServicePrincipal("emr-serverless.amazonaws.com")))
                        .actions(List.of("ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"))
                        .build());
            }
        }

        extraEcrImages.forEach(this::createRepository);
    }

    private Repository createRepository(String imageName) {
        return Repository.Builder.create(scope, "Repository-" + imageName)
                .repositoryName(SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(deploymentId) + "/" + imageName)
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
    }

    public static class Builder {
        private final Construct scope;
        private final String deploymentId;
        private List<String> extraEcrImages;

        private Builder(Construct scope, String deploymentId) {
            this.scope = scope;
            this.deploymentId = deploymentId;
        }

        public static Builder create(Construct scope, String deploymentId) {
            return new Builder(scope, deploymentId);
        }

        public Builder extraEcrImages(List<String> extraEcrImages) {
            this.extraEcrImages = extraEcrImages;
            return this;
        }

        public SleeperArtefactRepositories build() {
            return new SleeperArtefactRepositories(this);
        }
    }
}
