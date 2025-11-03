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
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketAccessControl;
import software.amazon.awscdk.services.s3.BucketEncryption;

public class SleeperArtefactsStack extends Stack {

    public SleeperArtefactsStack(App app, String id, StackProps props) {
        super(app, id, props);

        Bucket.Builder.create(this, "JarsBucket")
                .bucketName(id + "-jars")
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
    }

}
