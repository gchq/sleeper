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

package sleeper.systemtest.drivers.partitioning;

import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION;

public class AwsPartitionSplittingDriver implements PartitionSplittingDriver {

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;

    public AwsPartitionSplittingDriver(SystemTestInstanceContext instance, LambdaClient lambdaClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
    }

    public void splitPartitions() {
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION));
    }
}
