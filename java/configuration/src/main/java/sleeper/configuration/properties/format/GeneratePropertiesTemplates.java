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
package sleeper.configuration.properties.format;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.InstancePropertyGroup;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class GeneratePropertiesTemplates {

    private GeneratePropertiesTemplates() {
    }

    public static void main(String[] args) throws IOException {
        fromRepositoryPath(Path.of(args[0]));
    }

    public static void fromRepositoryPath(Path tempDir) throws IOException {
        InstanceProperties properties = new InstanceProperties();
        properties.set(ID, "full-example");
        properties.set(JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars");
        properties.set(ACCOUNT, "1234567890");
        properties.set(REGION, "eu-west-2");
        properties.set(VPC_ID, "1234567890");
        properties.set(SUBNET, "subnet-abcdefgh");
        properties.set(CONFIG_BUCKET, "test-bucket");
        Files.createDirectories(tempDir.resolve("example/full"));
        try (BufferedWriter writer = Files.newBufferedWriter(
                tempDir.resolve("example/full/instance.properties"))) {
            new SleeperPropertiesPrettyPrinter<InstanceProperty>(
                    Collections.unmodifiableList(UserDefinedInstanceProperty.getAll()),
                    InstancePropertyGroup.getAll(), new PrintWriter(writer))
                    .print(properties);
        }
    }
}
