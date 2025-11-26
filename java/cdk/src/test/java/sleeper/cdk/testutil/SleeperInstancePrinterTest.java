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
package sleeper.cdk.testutil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SleeperInstancePrinterTest {

    SleeperInstancePrinter printer = new SleeperInstancePrinter();
    Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    @Test
    void shouldRemoveTemplateURL() {
        // Given
        String templateSnippet = """
                {
                  "TemplateURL": {
                    "Fn::Join": [
                      "",
                      [
                        "https://s3.test-region.",
                        {
                            "Ref": "AWS::URLSuffix"
                        },
                        "/cdk-hnb659fds-assets-test-account-test-region/4330b5c344bc7b15668e1cdac745eeee2908bed02e74b0e53835aec8ef5e0527.json"
                      ]
                    ]
                  }
                }""";
        Map<String, Object> map = gson.fromJson(templateSnippet, Map.class);

        // When
        Map<String, Object> sanitised = printer.sanitiseTemplate(map);
        String printed = gson.toJson(sanitised);

        // Then
        assertThat(printed).isEqualTo("""
                {
                  "TemplateURL": "removed-for-test"
                }""");
    }

    @Test
    void shouldRemoveDateFromPropertiesJoin() {
        // Given
        String templateSnippet = """
                {
                  "properties": {
                    "Fn::Join": [
                      "",
                      [
                        "#\\n#Tue Nov 25 15:35:21 UTC 2025\\nsleeper.ingest.batcher.submit.dlq.arn\\u003d",
                        {
                          "Ref": "referencetoTestInstanceIngestBatcherNestedStackIngestBatcherNestedStackResource891B7171OutputsTestInstanceIngestBatcherIngestBatcherSubmitDLQ615E0449Arn"
                        },
                        "\\nsleeper.subnets\\u003dtest-subnet\\n"
                      ]
                    ]
                  }
                }""";
        Map<String, Object> map = gson.fromJson(templateSnippet, Map.class);

        // When
        Map<String, Object> sanitised = printer.sanitiseTemplate(map);
        String printed = gson.toJson(sanitised);

        // Then
        assertThat(printed).isEqualTo("""
                {
                  "properties": {
                    "Fn::Join": [
                      "",
                      [
                        "sleeper.ingest.batcher.submit.dlq.arn\\u003d",
                        {
                          "Ref": "referencetoTestInstanceIngestBatcherNestedStackIngestBatcherNestedStackResource891B7171OutputsTestInstanceIngestBatcherIngestBatcherSubmitDLQ615E0449Arn"
                        },
                        "\\nsleeper.subnets\\u003dtest-subnet\\n"
                      ]
                    ]
                  }
                }""");
    }

}
