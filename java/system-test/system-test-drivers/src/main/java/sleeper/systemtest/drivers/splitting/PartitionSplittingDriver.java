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

package sleeper.systemtest.drivers.splitting;

import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.core.statestore.StateStoreException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_LAMBDA_FUNCTION;

public class PartitionSplittingDriver {

    private final SleeperInstanceContext instance;
    private final LambdaClient lambdaClient;

    public PartitionSplittingDriver(SleeperInstanceContext instance, LambdaClient lambdaClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
    }

    public void splitPartitions() throws InterruptedException {
        try {
            WaitForPartitionSplitting waitForPartitionSplitting = WaitForPartitionSplitting
                    .forCurrentPartitionsNeedingSplitting(instance.getTableProperties(), instance.getStateStore());
            InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(PARTITION_SPLITTING_LAMBDA_FUNCTION));
            waitForPartitionSplitting.pollUntilFinished(instance.getStateStore());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
