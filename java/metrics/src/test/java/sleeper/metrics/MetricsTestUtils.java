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
package sleeper.metrics;

import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;

public class MetricsTestUtils {

    private MetricsTestUtils() {
    }

    public static String example(String path) {
        try (Reader reader = new InputStreamReader(MetricsTestUtils.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
