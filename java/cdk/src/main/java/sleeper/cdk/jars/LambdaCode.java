/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.cdk.jars;

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IVersion;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import java.util.function.Consumer;

public class LambdaCode {

    private final IBucket bucket;
    private final String filename;
    private final String versionId;

    public LambdaCode(IBucket bucket, String filename, String versionId) {
        this.bucket = bucket;
        this.filename = filename;
        this.versionId = versionId;
    }

    public IVersion buildFunction(Construct scope, String id, Consumer<Function.Builder> config) {

        Function.Builder builder = Function.Builder.create(scope, id)
                .code(Code.fromBucket(bucket, filename, versionId));
        config.accept(builder);
        Function function = builder.build();

        // This is needed to tell the CDK to update the functions with new code when it changes in the jars bucket.
        // See the following:
        // https://www.define.run/posts/cdk-not-updating-lambda/
        // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
        // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
        return function.getCurrentVersion();
    }
}
