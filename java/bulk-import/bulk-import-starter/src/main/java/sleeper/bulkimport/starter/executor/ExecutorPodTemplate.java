/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.bulkimport.starter.executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class ExecutorPodTemplate {

    private ExecutorPodTemplate() {
    }

    public static String forJobAndEphemeralStorageRequestAndLimit(String jobId, String request, String limit) {
        return loadTemplate()
                .replace("ephemeral-storage-request-placeholder", request)
                .replace("ephemeral-storage-limit-placeholder", limit)
                .replace("spark-app-selector-placeholder", jobId);
    }

    private static String loadTemplate() {
        try (InputStream is = ExecutorPodTemplate.class.getResourceAsStream("/executor-pod-template.yaml")) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
