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
package sleeper.systemtest.drivers.bulkexport;

import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.clients.api.BulkExportQuerySender;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.bulkexport.BulkExportDriver;

public class AwsBulkExportDriver implements BulkExportDriver {

    private SqsClient sqs;
    private InstanceProperties instanceProperties;

    public AwsBulkExportDriver(SystemTestContext context, SystemTestClients clients) {
        this.sqs = clients.getSqs();
        this.instanceProperties = context.instance().getInstanceProperties();
    }

    @Override
    public void sendJob(BulkExportQuery query) {
        //Fire off bulk export query to SQS
        BulkExportQuerySender sender = BulkExportQuerySender.toSqs(instanceProperties, sqs);
        sender.sendQueryToBulkExport(query);
    }
}
