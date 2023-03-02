/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.cdk;

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.VersionOptions;
import software.amazon.awscdk.services.s3.IBucket;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public final class LambdaCode {
    private final Code code;
    private final VersionOptions versionOptions;

    private LambdaCode(IBucket jarsBucket, String filename, String codeSha256) {
        this.code = Code.fromBucket(jarsBucket, filename);
        this.versionOptions = VersionOptions.builder()
                .codeSha256(codeSha256)
                .build();
    }

    public static LambdaCode from(BuiltJar jar, IBucket jarsBucket) {
        try {
            return new LambdaCode(jarsBucket, jar.fileName(), jar.codeSha256());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public IFunction buildFunction(Function.Builder builder) {
        return builder
                .code(code)
                .currentVersionOptions(versionOptions)
                .build()
                // This ensures that the latest version is output to the CloudFormation template.
                // See the following:
                // https://www.define.run/posts/cdk-not-updating-lambda/
                // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
                // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
                .getCurrentVersion();
    }
}
