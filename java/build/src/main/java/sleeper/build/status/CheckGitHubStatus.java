/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.status;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class CheckGitHubStatus {

    private CheckGitHubStatus() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <properties file path>");
        }
        String propertiesFile = args[0];
        ChunksStatus status = ProjectConfiguration.from(loadProperties(propertiesFile)).checkStatus();
        status.report(System.out);
        if (status.isFailCheck()) {
            System.exit(1);
        }
    }

    private static Properties loadProperties(String path) throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
            properties.load(reader);
        }
        return properties;
    }
}
