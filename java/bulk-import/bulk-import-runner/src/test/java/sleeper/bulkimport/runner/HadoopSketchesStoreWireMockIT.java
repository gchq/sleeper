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
package sleeper.bulkimport.runner;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.localstack.test.WiremockHadoopConfigurationProvider;
import sleeper.sketches.Sketches;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.findUnmatchedRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@WireMockTest
public class HadoopSketchesStoreWireMockIT {
    @TempDir
    public java.nio.file.Path folder;
    private HadoopSketchesStore sketchesSerDeToS3;
    private String file;
    private Schema schema;
    private Sketches sketches;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        schema = Schema.builder().rowKeyFields(
                new Field("key1", new IntType()),
                new Field("key2", new LongType()),
                new Field("key3", new StringType()),
                new Field("key4", new ByteArrayType()))
                .build();

        sketches = Sketches.from(schema);
        for (int i = 0; i < 100; i++) {
            sketches.update(new Record(Map.of(
                    "key1", i,
                    "key2", i + 1_000_000L,
                    "key3", "" + (i + 1_000_000L),
                    "key4", new byte[]{(byte) i, (byte) (i + 1)})));
        }

        sketchesSerDeToS3 = hadoopSketchesStore(runtimeInfo);

    }

    @Disabled
    @Test
    void shouldHandleNetworkErrorWhenSavingSketchesCorrectly(WireMockRuntimeInfo runtimeInfo) throws IOException {
        // Given
        // Mock UncheckedIOException error
        stubFor(put("/testbucket/file.sketches")
                .willReturn(aResponse().withStatus(500)));

        file = "s3a://testbucket/file.sketches";

        // When / Then
        assertThatThrownBy(() -> sketchesSerDeToS3.saveFileSketches(file, schema, sketches))
                .isInstanceOfSatisfying(UncheckedIOException.class,
                        e -> assertThat(e.getCause()).isInstanceOf(FileNotFoundException.class));

        assertThat(findUnmatchedRequests()).isEmpty();
    }

    private HadoopSketchesStore hadoopSketchesStore(WireMockRuntimeInfo runtimeInfo) {
        return new HadoopSketchesStore(WiremockHadoopConfigurationProvider.getHadoopConfiguration(runtimeInfo));
    }

}
