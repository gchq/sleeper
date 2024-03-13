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

import com.amazonaws.services.sqs.AmazonSQS;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.systemtest.dsl.gc.GarbageCollectionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_QUEUE_URL;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_TABLE_BATCH_SIZE;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECT_OFFLINE_TABLES;

public class AwsGarbageCollectionDriver implements GarbageCollectionDriver {

    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();
    private final SystemTestInstanceContext instance;
    private final LambdaClient lambdaClient;
    private final AmazonSQS sqsClient;

    public AwsGarbageCollectionDriver(SystemTestInstanceContext instance, LambdaClient lambdaClient, AmazonSQS sqsClient) {
        this.instance = instance;
        this.lambdaClient = lambdaClient;
        this.sqsClient = sqsClient;
    }

    @Override
    public void invokeGarbageCollectionForInstance() {
        InvokeLambda.invokeWith(lambdaClient, instance.getInstanceProperties().get(GARBAGE_COLLECTOR_LAMBDA_FUNCTION));
    }

    @Override
    public void sendGarbageCollection() {
        int batchSize = instance.getInstanceProperties().getInt(GARBAGE_COLLECTOR_TABLE_BATCH_SIZE);
        String queueUrl = instance.getInstanceProperties().get(GARBAGE_COLLECTOR_QUEUE_URL);
        boolean offlineEnabled = instance.getInstanceProperties().getBoolean(GARBAGE_COLLECT_OFFLINE_TABLES);
        InvokeForTableRequest.forTables(instance.streamTableProperties()
                .map(TableProperties::getStatus)
                .filter(table -> table.isOnline() || (!table.isOnline() && offlineEnabled)),
                batchSize, request -> sqsClient.sendMessage(queueUrl, serDe.toJson(request)));
    }
}
