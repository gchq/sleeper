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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.deploy.InvokeLambda;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasks;
import sleeper.systemtest.dsl.ingest.InvokeIngestTasksDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;

public class AwsInvokeIngestTasksDriver implements InvokeIngestTasksDriver {

    private final SystemTestInstanceContext instance;
    private final AmazonDynamoDB dynamoDBClient;
    private final LambdaClient lambdaClient;

    public AwsInvokeIngestTasksDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.dynamoDBClient = clients.getDynamoDB();
        this.lambdaClient = clients.getLambda();
    }

    @Override
    public InvokeIngestTasks invokeTasksForCurrentInstance() {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        IngestJobStatusStore statusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        return new InvokeIngestTasks(() -> {
            InvokeLambda.invokeWith(lambdaClient, instanceProperties.get(INGEST_LAMBDA_FUNCTION));
        }, statusStore);
    }
}
