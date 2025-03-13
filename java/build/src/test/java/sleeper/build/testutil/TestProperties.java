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
package sleeper.build.testutil;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import static sleeper.build.testutil.TestResources.exampleReader;

public class TestProperties {

    private TestProperties() {
    }

    public static Properties example(String path) {
        try (Reader reader = exampleReader(path)) {
            Properties properties = new Properties();
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load test example: " + path, e);
        }
    }
}
