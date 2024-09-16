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
package sleeper.build.testutil;

import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Objects;

public class TestResources {

    private TestResources() {
    }

    public static String exampleString(String path) {
        try (Reader reader = exampleReader(path)) {
            return CharStreams.toString(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load example: " + path, e);
        }
    }

    public static Reader exampleReader(String path) {
        return new InputStreamReader(exampleInputStream(path));
    }

    public static InputStream exampleInputStream(String path) {
        URL resource = Objects.requireNonNull(TestResources.class.getClassLoader().getResource(path));
        try {
            return resource.openStream();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load test example: " + path, e);
        }
    }
}
