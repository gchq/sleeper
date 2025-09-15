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

package sleeper.clients.deploy.localstack;

import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.runner.rowretrieval.QueryEngineSelector;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public abstract class DockerInstanceTestBase extends LocalStackTestBase {

    public void deployInstance(String instanceId) {
        deployInstance(instanceId, tableProperties -> {
        });
    }

    public void deployInstance(String instanceId, Consumer<TableProperties> extraProperties) {
        DeployDockerInstance.builder()
                .s3Client(s3Client)
                .dynamoClient(dynamoClient).sqsClient(sqsClient)
                .extraTableProperties(extraProperties)
                .build().deploy(instanceId);
    }

    public CloseableIterator<Row> queryAllRows(
            InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient)
                .getStateStore(tableProperties);
        PartitionTree tree = new PartitionTree(stateStore.getAllPartitions());
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties, stateStore,
                new QueryEngineSelector(Executors.newSingleThreadExecutor(), hadoopConf).getRowRetriever(tableProperties));
        executor.init(tree.getAllPartitions(), stateStore.getPartitionToReferencedFilesMap());
        return executor.execute(createQueryAllRows(tree, tableProperties.get(TABLE_NAME)));
    }

    protected IngestJob receiveIngestJob(String queueUrl) {
        List<Message> messages = sqsClient.receiveMessage(request -> request.queueUrl(queueUrl)).messages();
        if (messages.size() != 1) {
            throw new IllegalStateException("Expected to receive one message, found: " + messages);
        }
        String json = messages.get(0).body();
        return new IngestJobSerDe().fromJson(json);
    }

    private static Query createQueryAllRows(PartitionTree tree, String tableName) {
        return Query.builder()
                .tableName(tableName)
                .queryId(UUID.randomUUID().toString())
                .regions(List.of(tree.getRootPartition().getRegion()))
                .build();
    }
}
