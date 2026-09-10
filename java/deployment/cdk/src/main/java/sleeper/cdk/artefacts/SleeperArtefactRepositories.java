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
package sleeper.cdk.artefacts;

import org.apache.commons.lang3.EnumUtils;
import software.amazon.awscdk.CfnDeletionPolicy;
import software.amazon.awscdk.CfnResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.ICfnResourceOptions;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.LifecycleRule;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecr.TagStatus;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketAccessControl;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.model.SleeperArtefactsLocation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * AWS resources that will hold artefacts used to deploy Sleeper. This should be deployed separately before deploying a
 * Sleeper instance. The artefacts need to be uploaded as a separate step after deploying this, but before deploying a
 * Sleeper instance that requires those artefacts.
 */
public class SleeperArtefactRepositories {

    private final Construct scope;
    private final String deploymentId;
    private final ToDeploy deploy;
    private final IBucket jarsBucket;
    private final Map<LambdaJar, IRepository> jarToRepository = new HashMap<>();
    private final Map<DockerDeployment, IRepository> deploymentToRepository = new HashMap<>();

    private SleeperArtefactRepositories(Builder builder) {
        scope = Objects.requireNonNull(builder.scope, "scope must not be null");
        deploymentId = Objects.requireNonNull(builder.deploymentId, "deploymentId must not be null");
        String accountName = Objects.requireNonNull(builder.accountName, "accountName must not be null");
        deploy = Optional.ofNullable(builder.deploy).orElse(ToDeploy.ALL);
        jarsBucket = deploy.isDeployJars() ? createJarsBucket(scope, accountName, deploymentId, builder.retainLogsAfterDestroy) : null;
        if (deploy.isDeployImages()) {
            deployImages();
        }
    }

    public static IBucket createJarsBucket(Construct scope, String accountName, String deploymentId) {
        return createJarsBucket(scope, accountName, deploymentId, true);
    }

    /**
     * Creates a versioned jars bucket with managed logs for its cleanup function.
     *
     * @param  scope                  the scope for the bucket
     * @param  accountName            the AWS account
     * @param  deploymentId           the artefacts deployment ID
     * @param  retainLogsAfterDestroy whether to retain cleanup logs after stack deletion
     * @return                        the jars bucket
     */
    public static IBucket createJarsBucket(Construct scope, String accountName, String deploymentId, boolean retainLogsAfterDestroy) {
        IBucket bucket = Bucket.Builder.create(scope, "JarsBucket")
                .bucketName(SleeperArtefactsLocation.getDefaultJarsBucketName(accountName, deploymentId))
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
        configureCleanupLogs(scope, retainLogsAfterDestroy);
        return bucket;
    }

    private static void configureCleanupLogs(Construct scope, boolean retainLogsAfterDestroy) {
        // Keep the existing provider and custom resource so upgrading does not replace the bucket cleanup resource.
        Construct provider = (Construct) Stack.of(scope).getNode().findChild("Custom::S3AutoDeleteObjectsCustomResourceProvider");
        CfnResource handler = (CfnResource) provider.getNode().findChild("Handler");
        CfnDeletionPolicy deletionPolicy = retainLogsAfterDestroy ? CfnDeletionPolicy.RETAIN : CfnDeletionPolicy.DELETE;
        LogGroup logGroup = (LogGroup) provider.getNode().tryFindChild("LogGroup");
        if (logGroup == null) {
            logGroup = LogGroup.Builder.create(provider, "LogGroup")
                    .retention(RetentionDays.INFINITE)
                    .removalPolicy(retainLogsAfterDestroy ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY)
                    .build();
            // Referencing the group keeps it alive until the cleanup function has been deleted.
            handler.addPropertyOverride("LoggingConfig.LogGroup", logGroup.getLogGroupName());
        } else {
            CfnResource logResource = (CfnResource) logGroup.getNode().getDefaultChild();
            if (logResource == null) {
                throw new IllegalStateException("Cleanup log group has no CloudFormation resource");
            }
            ICfnResourceOptions options = logResource.getCfnOptions();
            if (options == null) {
                throw new IllegalStateException("Cleanup log group has no CloudFormation options");
            }
            if (!Objects.equals(deletionPolicy, options.getDeletionPolicy())) {
                throw new IllegalArgumentException("Buckets sharing a cleanup provider must use the same log retention policy");
            }
        }
    }

    public String getDeploymentId() {
        return deploymentId;
    }

    public IBucket getJarsBucket() {
        return Objects.requireNonNull(jarsBucket, "Jars bucket not deployed");
    }

    public IRepository getRepository(LambdaJar jar) {
        return Objects.requireNonNull(jarToRepository.get(jar), "Repository not deployed for jar: " + jar);
    }

    public IRepository getRepository(DockerDeployment deployment) {
        return Objects.requireNonNull(deploymentToRepository.get(deployment), "Repository not deployed for deployment: " + deployment);
    }

    private void deployImages() {
        for (LambdaJar jar : LambdaJar.all()) {
            jarToRepository.put(jar, createRepository(jar.getImageName()));
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
            deploymentToRepository.put(deployment, repository);
        }
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
        private String accountName;
        private ToDeploy deploy;
        private boolean retainLogsAfterDestroy = true;

        private Builder(Construct scope, String deploymentId) {
            this.scope = scope;
            this.deploymentId = deploymentId;
            if (scope instanceof Stack stack) {
                accountName = stack.getAccount();
            }
        }

        public static Builder create(Construct scope, String deploymentId) {
            return new Builder(scope, deploymentId);
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder deploy(ToDeploy deploy) {
            this.deploy = deploy;
            return this;
        }

        /**
         * Sets whether cleanup logs are retained when the artefacts stack is destroyed. Defaults to true.
         *
         * @param  retainLogsAfterDestroy whether to retain the logs
         * @return                        this builder
         */
        public Builder retainLogsAfterDestroy(boolean retainLogsAfterDestroy) {
            this.retainLogsAfterDestroy = retainLogsAfterDestroy;
            return this;
        }

        public SleeperArtefactRepositories build() {
            return new SleeperArtefactRepositories(this);
        }
    }

    public enum ToDeploy {
        ALL, JARS, IMAGES;

        public static ToDeploy fromString(String string) {
            if (string == null) {
                return null;
            }
            ToDeploy upload = EnumUtils.getEnumIgnoreCase(ToDeploy.class, string);
            if (upload == null) {
                throw new IllegalArgumentException("Unknown identifier for which resources to deploy: " + string);
            }
            return upload;
        }

        public boolean isDeployJars() {
            return this == ALL || this == JARS;
        }

        public boolean isDeployImages() {
            return this == ALL || this == IMAGES;
        }
    }
}
