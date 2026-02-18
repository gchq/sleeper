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
package sleeper.cdk.artefacts.containers;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.constructs.Construct;

import sleeper.cdk.artefacts.UploadArtefactsAssets;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

public class CopyContainerImageStack extends NestedStack {

    public CopyContainerImageStack(Construct scope, String id, InstanceProperties instanceProperties, UploadArtefactsAssets assets, LoggingStack loggingStack) {
        super(scope, id);
        create(Utils.cleanInstanceId(instanceProperties), assets,
                loggingStack.getLogGroup(LogGroupRef.COPY_CONTAINER),
                loggingStack.getLogGroup(LogGroupRef.COPY_CONTAINER_PROVIDER));
        Utils.addTags(this, instanceProperties);
    }

    private void create(String deploymentId, UploadArtefactsAssets assets, ILogGroup lambdaLogGroup, ILogGroup providerLogGroup) {
        assets.buildFunction(this, "Function", LambdaHandler.COPY_CONTAINER, builder -> builder
                .functionName("sleeper-" + deploymentId + "-copy-container")
                .memorySize(2048)
                .description("Lambda for copying container images from an external repository to ECR")
                .logGroup(lambdaLogGroup)
                .timeout(Duration.minutes(15)));
    }

}
