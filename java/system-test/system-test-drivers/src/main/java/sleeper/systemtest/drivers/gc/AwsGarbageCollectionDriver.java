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

package sleeper.systemtest.drivers.gc;

import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_FUNCTION;

public class AwsGarbageCollectionDriver implements GarbageCollectionDriver {

    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;

    public AwsGarbageCollectionDriver(SystemTestInstanceContext instance, LambdaClient lambdaClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
    }

    @Override
    public void invokeGarbageCollection() {
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(GARBAGE_COLLECTOR_LAMBDA_FUNCTION));
    }

}
