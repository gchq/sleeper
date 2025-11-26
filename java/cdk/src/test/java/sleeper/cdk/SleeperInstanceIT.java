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
package sleeper.cdk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;
import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.assertions.Template;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;

public class SleeperInstanceIT {

    Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();
    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("test-instance");

    @Test
    void shouldGenerateCloudFormationTemplateWithDefaultStacks() {
        // Given
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account("test-account")
                .region("test-region")
                .build();
        StackProps stackProps = StackProps.builder()
                .stackName(instanceProperties.get(ID))
                .env(environment)
                .build();
        SleeperInstanceProps sleeperProps = SleeperInstanceProps.builder()
                .instanceProperties(instanceProperties)
                .jars(jarsInBucket())
                .skipCheckingVersionMatchesProperties(true)
                .build();

        // When
        Stack stack = SleeperInstance.createAsRootStack(app, "TestInstance", stackProps, sleeperProps);

        // Then
        Approvals.verify(toJson(stack), new Options()
                .withReporter((receieved, approved) -> false) // Generating diff output for failures is too slow, so skip it
                .forFile().withName("default-instance", ".json"));
    }

    private SleeperJarsInBucket jarsInBucket() {
        return SleeperJarsInBucket.from(jar -> jar.getArtifactId() + "-test-version", instanceProperties);
    }

    private String toJson(Stack stack) {
        List<Stack> stacks = stack.getNode().findAll().stream()
                .filter(construct -> construct instanceof Stack)
                .map(construct -> (Stack) construct)
                .toList();
        List<Map<String, Object>> maps = stacks.stream().map(this::toSanitisedMap).toList();
        return gson.toJson(maps);
    }

    private Map<String, Object> toSanitisedMap(Stack stack) {
        Map<String, Object> map = Template.fromStack(stack).toJSON();
        return sanitise(map);
    }

    private Map<String, Object> sanitise(Map<String, Object> map) {
        return map.entrySet().stream()
                .map(this::sanitise)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    private Entry<String, Object> sanitise(Entry<String, Object> entry) {
        if (Objects.equals("TemplateURL", entry.getKey())) {
            return Map.entry(entry.getKey(), "removed-for-test");
        }

        if (Objects.equals("properties", entry.getKey())
                && entry.getValue() instanceof Map map
                && Objects.equals(Set.of("Fn::Join"), map.keySet())
                && map.get("Fn::Join") instanceof List list) {
            return Map.entry(entry.getKey(), Map.of("Fn::Join", sanitisePropertiesJoin(list)));
        }

        if (entry.getValue() instanceof Map map) {
            return Map.entry(entry.getKey(), sanitise(map));
        }

        return entry;
    }

    private List<Object> sanitisePropertiesJoin(List<Object> list) {
        return list.stream().map(this::sanitisePropertiesJoinElement).toList();
    }

    private static final Pattern DATE_COMMENT = Pattern.compile("#\\n#\\w+ \\w+ \\w+ \\w+:\\w+:\\w+ UTC \\w+\\n");

    private Object sanitisePropertiesJoinElement(Object element) {
        if (element instanceof List list) {
            return sanitisePropertiesJoin(list);
        }
        if (element instanceof String string) {
            Matcher matcher = DATE_COMMENT.matcher(string);
            if (matcher.find() && matcher.start() == 0) {
                return string.substring(matcher.end());
            }
            return string;
        }
        return element;
    }

    @Test
    void shouldSanitisePropertiesJoin() {
        // Given
        String exampleProperties = """
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
        Map<String, Object> map = gson.fromJson(exampleProperties, Map.class);

        // When
        Map<String, Object> sanitised = sanitise(map);
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
