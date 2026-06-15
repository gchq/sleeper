/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;

/**
 * Sends a query to a Sleeper table. The query must be tracked and results retrieved separately.
 */
public interface QuerySender {

    /**
     * Sends a query against a Sleeper table.
     *
     * @param query the query
     */
    void sendQuery(Query query);

    /**
     * Creates a sender that will submit queries to an SQS queue.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider the table properties provider
     * @param  sqsClient               the SQS client
     * @return                         the sender
     */
    static QuerySender toSqs(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, SqsClient sqsClient) {
        QuerySerDe querySerDe = new QuerySerDe(tablePropertiesProvider);
        return query -> {
            sqsClient.sendMessage(request -> request.queueUrl(instanceProperties.get(QUERY_QUEUE_URL))
                    .messageBody(querySerDe.toJson(query)));
        };
    }

}
