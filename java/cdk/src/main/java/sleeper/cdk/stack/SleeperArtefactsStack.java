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
package sleeper.cdk.stack;

import software.amazon.awscdk.App;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketAccessControl;
import software.amazon.awscdk.services.s3.BucketEncryption;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaJar;

import java.util.List;

/**
 * A CDK stack to deploy AWS resources that will hold artefacts used to deploy Sleeper. This should be deployed
 * separately before deploying a Sleeper instance. The artefacts need to be uploaded as a separate step after deploying
 * this stack, but before deploying a Sleeper instance that requires those artefacts.
 */
public class SleeperArtefactsStack extends Stack {

    public SleeperArtefactsStack(App app, String stackId, StackProps props, String deploymentId, List<String> extraEcrImages) {
        super(app, stackId, props);

        Bucket.Builder.create(this, "JarsBucket")
                .bucketName(String.join("-", "sleeper", deploymentId, "jars"))
                .encryption(BucketEncryption.S3_MANAGED)
                .accessControl(BucketAccessControl.PRIVATE)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                // We enable versioning so that the CDK is able to update functions when the code changes in the bucket.
                // See the following:
                // https://www.define.run/posts/cdk-not-updating-lambda/
                // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
                // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
                .versioned(true)
                .build();

        for (LambdaJar jar : LambdaJar.all()) {
            createRepository(deploymentId, jar.getImageName());
        }

        for (DockerDeployment deployment : DockerDeployment.all()) {
            Repository repository = createRepository(deploymentId, deployment.getDeploymentName());

            if (deployment.isCreateEmrServerlessPolicy()) {
                repository.addToResourcePolicy(PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .principals(List.of(new ServicePrincipal("emr-serverless.amazonaws.com")))
                        .actions(List.of("ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"))
                        .build());
            }
        }

        for (String imageName : extraEcrImages) {
            createRepository(deploymentId, imageName);
        }
    }

    private Repository createRepository(String deploymentId, String imageName) {
        return Repository.Builder.create(this, "Repository-" + imageName)
                .repositoryName(String.join("-", "sleeper", deploymentId) + "/" + imageName)
                .build();
    }

}
