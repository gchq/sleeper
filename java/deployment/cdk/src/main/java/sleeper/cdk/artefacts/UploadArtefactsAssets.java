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

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IVersion;
import software.amazon.awscdk.services.lambda.Runtime;
import software.constructs.Construct;

import sleeper.cdk.lambda.FunctionBuilder;
import sleeper.cdk.lambda.LambdaBuilder;
import sleeper.core.SleeperVersion;
import sleeper.core.deploy.LambdaHandler;

import java.nio.file.Path;
import java.util.function.Consumer;

public record UploadArtefactsAssets(Path jarsDirectory, String version) {

    public static UploadArtefactsAssets fromJarsDirectory(Path jarsDirectory) {
        return new UploadArtefactsAssets(jarsDirectory, SleeperVersion.getVersion());
    }

    public Code lambdaCode(LambdaHandler handler) {
        String filename = handler.getJar().getFilename(version);
        String path = jarsDirectory.resolve(filename).toAbsolutePath().toString();
        return Code.fromAsset(path);
    }

    public IVersion buildFunction(Construct scope, String id, LambdaHandler handler, Consumer<LambdaBuilder> config) {
        LambdaBuilder builder = new FunctionBuilder(Function.Builder.create(scope, id)
                .code(lambdaCode(handler))
                .handler(handler.getHandler())
                .runtime(Runtime.JAVA_17));
        config.accept(builder);
        return builder.build().getCurrentVersion();
    }

}
