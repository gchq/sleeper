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
package sleeper.bulkimport.runner.common;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hadoop.fs.s3a.AWSStatus500Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.localstack.test.WiremockHadoopConfigurationProvider;
import sleeper.sketches.Sketches;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.findUnmatchedRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

@WireMockTest
public class HadoopSketchesStoreWireMockIT {
    private HadoopSketchesStore sketchesStore;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        sketchesStore = hadoopSketchesStore(runtimeInfo);
    }

    @Test
    void shouldHandleNetworkErrorWhenSavingSketches(WireMockRuntimeInfo runtimeInfo) throws IOException {
        // Given
        // Mock server error
        stubFor(get("/testbucket?list-type=2&delimiter=%2F&max-keys=2&prefix=file.sketches%2F")
                .willReturn(aResponse().withStatus(500)));

        String file = "s3a://testbucket/file.parquet";
        Schema schema = createSchemaWithKey("key");
        Sketches sketches = Sketches.from(schema);

        // When / Then
        assertThatThrownBy(() -> sketchesStore.saveFileSketches(file, schema, sketches))
                .isInstanceOf(UncheckedIOException.class)
                .cause().isInstanceOf(AWSStatus500Exception.class);
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    @Test
    void shouldHandleNetworkErrorWhenLoadingSketches(WireMockRuntimeInfo runtimeInfo) throws IOException {
        // Given
        // Mock server error
        stubFor(head(urlEqualTo("/testbucket/file.sketches"))
                .willReturn(aResponse().withStatus(500)));

        String file = "s3a://testbucket/file.parquet";
        Schema schema = createSchemaWithKey("key");

        // When / Then
        assertThatThrownBy(() -> sketchesStore.loadFileSketches(file, schema))
                .isInstanceOf(UncheckedIOException.class)
                .cause().isInstanceOf(AWSStatus500Exception.class);
        assertThat(findUnmatchedRequests()).isEmpty();
    }

    private HadoopSketchesStore hadoopSketchesStore(WireMockRuntimeInfo runtimeInfo) {
        return new HadoopSketchesStore(WiremockHadoopConfigurationProvider.getHadoopConfiguration(runtimeInfo));
    }

}
