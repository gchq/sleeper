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
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

public class TestProperties {

    private TestProperties() {
    }

    public static Properties example(String path) throws IOException {
        URL resource = Objects.requireNonNull(ChunksStatusTest.class.getClassLoader().getResource(path));
        try (InputStream is = resource.openStream()) {
            Properties properties = new Properties();
            properties.load(is);
            return properties;
        }
    }
}
