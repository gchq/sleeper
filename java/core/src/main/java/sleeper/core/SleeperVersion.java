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
package sleeper.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Retrieves the version number of Sleeper currently being used.
 */
public class SleeperVersion {

    private SleeperVersion() {
        // Prevent instantiation
    }

    private static final String VERSION = loadVersion();

    public static String getVersion() {
        return VERSION;
    }

    private static String loadVersion() {
        URL resource = SleeperVersion.class.getClassLoader().getResource("version.txt");
        if (resource == null) {
            throw new IllegalStateException("Sleeper version not found");
        }
        try (InputStream inputStream = resource.openStream()) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
