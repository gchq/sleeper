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

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunksStatusTest {

    @Test
    public void canFindTwoChunksSuccessful() throws Exception {
        Properties properties = exampleProperties("two-chunks-successful.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.success("data")));
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, success\n");
    }

    @Test
    public void canFindOneChunkSuccessfulOneInProgress() throws Exception {
        Properties properties = exampleProperties("one-chunk-successful-one-in-progress.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.inProgress("data")));
        assertThat(status.isFailCheck()).isFalse();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: in_progress\n");
    }

    @Test
    public void canFindOneChunkSuccessfulOneFailed() throws Exception {
        Properties properties = exampleProperties("one-chunk-successful-one-failed.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.failure("data")));
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, failure\n");
    }

    @Test
    public void canFindOneChunkSuccessfulOneCancelled() throws Exception {
        Properties properties = exampleProperties("one-chunk-successful-one-cancelled.properties");
        ChunksStatus status = ChunksStatus.from(properties);

        assertThat(status).isEqualTo(
                ChunksStatus.chunks(
                        ChunkStatus.success("common"),
                        ChunkStatus.cancelled("data")));
        assertThat(status.isFailCheck()).isTrue();
        assertThat(status.reportString()).isEqualTo("" +
                "common: completed, success\n" +
                "data: completed, cancelled\n");
    }

    private static Properties exampleProperties(String path) throws IOException {
        URL resource = Objects.requireNonNull(ChunksStatusTest.class.getClassLoader().getResource(path));
        try (InputStream is = resource.openStream()) {
            Properties properties = new Properties();
            properties.load(is);
            return properties;
        }
    }
}
