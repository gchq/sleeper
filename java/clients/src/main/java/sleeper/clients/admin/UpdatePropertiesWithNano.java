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
package sleeper.clients.admin;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class UpdatePropertiesWithNano {

    private final Path tempDirectory;

    public UpdatePropertiesWithNano(Path tempDirectory) {
        this.tempDirectory = tempDirectory;
    }

    public void updateProperties(InstanceProperties properties, RunCommand runCommand) throws IOException, InterruptedException {
        Files.createDirectories(tempDirectory.resolve("sleeper/admin"));
        Path propertiesFile = tempDirectory.resolve("sleeper/admin/instance.properties");
        properties.save(propertiesFile);
        runCommand.run("nano", propertiesFile.toString());
    }
}
