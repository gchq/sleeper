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

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.core.properties.table.TableProperties;
import sleeper.invoke.tables.InvokeForTables;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.partitioning.PartitionSplittingDriver;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_QUEUE_URL;

public class AwsPartitionSplittingDriver implements PartitionSplittingDriver {

    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqs;

    public AwsPartitionSplittingDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.sqs = clients.getSqs();
    }

    public void splitPartitions() {
        String queueUrl = instance.getInstanceProperties().get(FIND_PARTITIONS_TO_SPLIT_QUEUE_URL);
        InvokeForTables.sendOneMessagePerTable(sqs, queueUrl, instance.streamTableProperties().map(TableProperties::getStatus));
    }
}
