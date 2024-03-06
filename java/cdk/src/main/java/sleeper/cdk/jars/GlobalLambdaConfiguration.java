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

import software.amazon.awscdk.services.lambda.LayerVersion;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.TRACING_ENABLED;

public class GlobalLambdaConfiguration implements LambdaBuilder.Configuration {

    private final InstanceProperties instanceProperties;

    public GlobalLambdaConfiguration(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void apply(Construct scope, String functionId, LambdaBuilder builder) {
        builder.environmentVariables(Utils.createDefaultEnvironment(instanceProperties));
        if (instanceProperties.getBoolean(TRACING_ENABLED)) {
            String region = instanceProperties.get(REGION);
            String arn = "arn:aws:lambda:" + region + ":901920570463:layer:aws-otel-java-agent-amd64-ver-1-32-0:1";
            builder.layer(LayerVersion.fromLayerVersionArn(scope, functionId + "Tracing", arn));
            builder.environmentVariable("AWS_LAMBDA_EXEC_WRAPPER", "/opt/otel-handler");
        }
    }

}
