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

import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.ILayerVersion;
import software.amazon.awscdk.services.lambda.IVersion;
import software.constructs.Construct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class LambdaBuilder {
    private final Function.Builder builder;
    private final Map<String, String> environment = new HashMap<>();
    private final List<ILayerVersion> layers = new ArrayList<>();

    public LambdaBuilder(Function.Builder builder) {
        this.builder = builder;
    }

    public LambdaBuilder config(Consumer<Function.Builder> config) {
        config.accept(builder);
        return this;
    }

    public LambdaBuilder environmentVariable(String key, String value) {
        this.environment.put(key, value);
        return this;
    }

    public LambdaBuilder environmentVariables(Map<String, String> environment) {
        this.environment.putAll(environment);
        return this;
    }

    public LambdaBuilder layer(ILayerVersion layer) {
        layers.add(layer);
        return this;
    }

    public IVersion build() {
        Function function = builder.environment(environment).layers(layers).build();

        // This is needed to tell the CDK to update the functions with new code when it changes in the jars bucket.
        // See the following:
        // https://www.define.run/posts/cdk-not-updating-lambda/
        // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
        // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
        return function.getCurrentVersion();
    }

    public interface Configuration {
        void apply(Construct scope, String functionId, LambdaBuilder builder);

        static Configuration none() {
            return (scope, functionId, builder) -> {
            };
        }
    }
}
