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
package sleeper.bulkimport.job;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkImportJobSerDeTest {

    @Test
    public void shouldDeserialiseEmptyJob() {
        // Given / When
        BulkImportJob bulkImportJob = new BulkImportJobSerDe().fromJson("{}");

        // Then
        assertThat(bulkImportJob)
                .isEqualTo(new BulkImportJob.Builder().build());
    }

    @Test
    public void shouldDeserialiseFullBulkImportJob() {
        // Given
        HashMap<String, String> sparkConf = new HashMap<>();
        sparkConf.put("key", "value");
        BulkImportJob expected = new BulkImportJob.Builder()
                .id("myJob")
                .sparkConf(sparkConf)
                .className("com.example.MyClass")
                .files(Lists.newArrayList("a/b/c.parquet"))
                .build();

        // When
        BulkImportJob bulkImportJob = new BulkImportJobSerDe().fromJson("" +
                "{" +
                "   \"id\": \"myJob\"," +
                "   \"className\": \"com.example.MyClass\"," +
                "   \"files\": [ \"a/b/c.parquet\" ]," +
                "   \"sparkConf\": {" +
                "       \"key\": \"value\"" +
                "   }" +
                "}");

        // Then
        assertThat(bulkImportJob).isEqualTo(expected);
    }

    @Test
    public void shouldSerialiseEmptyJob() {
        // Given
        BulkImportJob emptyJob = new BulkImportJob.Builder().id("empty-job").build();

        // When
        String serialised = new BulkImportJobSerDe().toJson(emptyJob);

        // Then
        assertThat(serialised).isEqualTo("{\"id\":\"empty-job\"}");
    }

    @Test
    public void shouldSerialiseFullJob() {
        // Given
        HashMap<String, String> sparkConf = new HashMap<>();
        sparkConf.put("key", "value");
        BulkImportJob fullJob = new BulkImportJob.Builder()
                .id("myJob")
                .sparkConf(sparkConf)
                .className("com.example.MyClass")
                .files(Lists.newArrayList("a/b/c.parquet"))
                .build();

        // When
        String serialised = new BulkImportJobSerDe().toJson(fullJob);

        // Then
        assertThatJson(serialised).isEqualTo("" +
                "{" +
                "    \"className\":\"com.example.MyClass\"," +
                "    \"files\":[\"a/b/c.parquet\"]," +
                "    \"id\":\"myJob\"," +
                "    \"sparkConf\":{" +
                "        \"key\":\"value\"" +
                "    }" +
                "}");
    }

    @Test
    public void shouldBeAbleToDeserialiseSerialisedJob() {
        // Given
        HashMap<String, String> sparkConf = new HashMap<>();
        sparkConf.put("key", "value");
        BulkImportJob fullJob = new BulkImportJob.Builder()
                .id("myJob")
                .sparkConf(sparkConf)
                .className("com.example.MyClass")
                .files(Lists.newArrayList("a/b/c.parquet"))
                .platformSpec(sparkConf)
                .build();

        // When
        BulkImportJobSerDe serDe = new BulkImportJobSerDe();
        BulkImportJob job = serDe.fromJson(serDe.toJson(fullJob));

        // Then
        assertThat(job).isEqualTo(fullJob);
    }
}
