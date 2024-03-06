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
    private final LambdaBuilder.Configuration globalConfig;

    LambdaCode(IBucket bucket, String filename, String versionId, LambdaBuilder.Configuration globalConfig) {
        this.bucket = bucket;
        this.filename = filename;
        this.versionId = versionId;
        this.globalConfig = globalConfig;
    }

    public IVersion buildFunction(Construct scope, String id, Consumer<Function.Builder> config) {
        return createFunction(scope, id).config(config).build();
    }

    public LambdaBuilder createFunction(Construct scope, String id) {
        LambdaBuilder builder = new LambdaBuilder(Function.Builder.create(scope, id)
                .code(Code.fromBucket(bucket, filename, versionId)));
        globalConfig.apply(scope, id, builder);
        return builder;
    }
}
