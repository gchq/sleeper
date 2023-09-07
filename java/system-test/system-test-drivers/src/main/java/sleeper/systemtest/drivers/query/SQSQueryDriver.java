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

package sleeper.systemtest.drivers.query;

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.Map;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;

public class SQSQueryDriver {
    private final String queueUrl;
    private final AmazonSQS sqsClient;
    private final QuerySerDe querySerDe;

    public SQSQueryDriver(SleeperInstanceContext instance, AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
        this.queueUrl = instance.getInstanceProperties().get(QUERY_QUEUE_URL);
        this.querySerDe = new QuerySerDe(Map.of(instance.getTableName(), instance.getTableProperties().getSchema()));
    }

    public void run(Query query) {
        sqsClient.sendMessage(queueUrl, querySerDe.toJson(query));
    }
}
