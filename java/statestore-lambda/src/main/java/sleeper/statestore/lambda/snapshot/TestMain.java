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
package sleeper.statestore.lambda.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreArrowFileStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Test.
 */
public class TestMain {
    public static final Logger LOGGER = LoggerFactory.getLogger(TestMain.class);

    private TestMain() {
    }

    public static void main(String[] args) throws IOException {
        Path configDir = Paths.get(Objects.requireNonNull(System.getenv("CONFIG_DIR"), "CONFIG_DIR must be set"));
        String snapshotFile = Objects.requireNonNull(System.getenv("SNAPSHOT_FILE"), "SNAPSHOT_FILE must be set");
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(configDir);
        TableProperties tableProperties = LoadLocalProperties.loadTablesFromDirectory(instanceProperties, configDir).findFirst().orElseThrow();
        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForClient();
        StateStoreArrowFileStore fileStore = new StateStoreArrowFileStore(tableProperties, hadoopConf);
        LOGGER.info("Loading from file: {}", snapshotFile);
        List<Partition> partitions = fileStore.loadPartitions(snapshotFile);
        LOGGER.info("Loaded partitions: {}", partitions);

        Path tempDir = Files.createTempDirectory("sleeper-test");
        String outputFile = tempDir.resolve(UUID.randomUUID().toString()).toString();
        LOGGER.info("Saving to file: {}", outputFile);
        fileStore.savePartitions(outputFile, partitions);
        LOGGER.info("Reloading file: {}", outputFile);
        List<Partition> found = fileStore.loadPartitions(outputFile);
        LOGGER.info("Reloaded partitions: {}", found);
    }

}
