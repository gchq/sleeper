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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;

public class GeneratePropertiesTemplates {

    private GeneratePropertiesTemplates() {
    }

    public static void fromRepositoryPath(Path tempDir) throws IOException {
        InstanceProperties properties = new InstanceProperties();
        properties.set(ID, "full-example");
        properties.set(JARS_BUCKET, "the name of the bucket containing your jars, e.g. sleeper-<insert-unique-name-here>-jars");
        Files.createDirectories(tempDir.resolve("example/full"));
        try (BufferedWriter writer = Files.newBufferedWriter(
                tempDir.resolve("example/full/instance.properties"))) {
            SleeperPropertiesPrettyPrinter.forInstanceProperties(new PrintWriter(writer))
                    .print(properties);
        }
    }
}
