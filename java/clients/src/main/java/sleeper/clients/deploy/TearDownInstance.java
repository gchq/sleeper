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
package sleeper.clients.deploy;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.util.ClientUtils.optionalArgument;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private TearDownInstance() {
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts directory> <optional instance id>");
        }
        Path scriptsDir = Path.of(args[0]);
        Path generatedDir = scriptsDir.resolve("generated");
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = loadInstanceConfig(
                optionalArgument(args, 1).orElse(null),
                generatedDir, s3);

        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("generatedDir: {}", generatedDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceProperties.get(ID));
        LOGGER.info("{}: {}", CONFIG_BUCKET.getPropertyName(), instanceProperties.get(CONFIG_BUCKET));
        LOGGER.info("{}: {}", QUERY_RESULTS_BUCKET.getPropertyName(), instanceProperties.get(QUERY_RESULTS_BUCKET));

        List<TableProperties> tablePropertiesList = LoadLocalProperties
                .loadTablesFromDirectory(instanceProperties, scriptsDir).collect(Collectors.toList());
        AmazonECS ecs = AmazonECSClientBuilder.defaultClient();
        new CleanUpBeforeDestroy(s3, ecs).cleanUp(instanceProperties, tablePropertiesList, List.of());
    }

    private static InstanceProperties loadInstanceConfig(
            @Nullable String instanceIdArg, Path generatedDir, AmazonS3 s3) throws IOException {
        String instanceId;
        if (instanceIdArg == null) {
            InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(generatedDir);
            instanceId = instanceProperties.get(ID);
        } else {
            instanceId = instanceIdArg;
        }
        LOGGER.info("Updating configuration for instance {}", instanceId);
        return SaveLocalProperties.saveFromS3(s3, instanceId, generatedDir);
    }
}
