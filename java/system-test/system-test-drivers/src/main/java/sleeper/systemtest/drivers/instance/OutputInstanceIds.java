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

package sleeper.systemtest.drivers.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class OutputInstanceIds {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputInstanceIds.class);

    private OutputInstanceIds() {
    }

    public static void addInstanceIdToOutput(String instanceId, SystemTestParameters parameters) {
        Path outputDirectory = parameters.getOutputDirectory();
        if (outputDirectory == null) {
            return;
        }
        try {
            Path outputFile = outputDirectory.resolve("instanceIds.txt");
            LOGGER.info("Appending instance id {} to output file: {}", instanceId, outputFile);
            Files.writeString(outputFile, instanceId + "\n", CREATE, APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
