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
package sleeper.clients.api;

import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkexport.core.model.BulkExportQuerySerDe;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;

@FunctionalInterface
public interface BulkExportQuerySender {

    void sendQueryToBulkExport(BulkExportQuery query);

    static BulkExportQuerySender toSqs(InstanceProperties instanceProperties, SqsClient sqsClient) {
        BulkExportQuerySerDe serDe = new BulkExportQuerySerDe();
        return query -> sqsClient.sendMessage(send -> send
                .queueUrl(instanceProperties.get(BULK_EXPORT_QUEUE_URL))
                .messageBody(serDe.toJson(query)));
    }
}
