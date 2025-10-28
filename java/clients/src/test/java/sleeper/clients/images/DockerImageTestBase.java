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
package sleeper.clients.images;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;

import sleeper.clients.util.command.CommandUtils;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.EnvironmentUtils;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static sleeper.clients.util.command.Command.command;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public abstract class DockerImageTestBase extends LocalStackTestBase {

    protected InstanceProperties instanceProperties = createTestInstanceProperties();
    protected TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new LongType()));
    protected StateStore stateStore;

    @BeforeEach
    void setUpBase() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    protected void runDockerImage(String dockerImage, String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.addAll(List.of("docker", "run", "--rm", "-it", "--network=host"));

        Map<String, String> environment = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        environment.put("AWS_ENDPOINT_URL", localStackContainer.getEndpoint().toString());
        environment.forEach((variable, value) -> command.addAll(List.of("--env", variable + "=" + value)));

        command.add(dockerImage);
        command.addAll(List.of(arguments));

        CommandUtils.runCommandLogOutputWithPty(command(command.toArray(String[]::new)));
    }

    protected FileReference addFileAtRoot(String name, List<Row> rows) {
        FileReference reference = fileFactory().rootFile(name, rows.size());
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(reference.getFilename());
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, tableProperties, hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        update(stateStore).addFile(reference);
        return reference;
    }

    protected List<Row> readFile(String filename) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filename);
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(path, tableProperties.getSchema()).withConf(hadoopConf).build())) {
            List<Row> rows = new ArrayList<>();
            reader.forEachRemaining(rows::add);
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore);
    }

}
