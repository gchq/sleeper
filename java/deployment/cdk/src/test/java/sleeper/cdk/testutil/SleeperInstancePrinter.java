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
package sleeper.cdk.testutil;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import software.amazon.awscdk.Stack;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toMap;

/**
 * A test helper to print a CDK stack containing a Sleeper instance, for use in approval tests.
 */
public class SleeperInstancePrinter {

    private final StackPrinter printer = StackPrinter.sanitiseTemplates(this::sanitiseTemplate);

    public String toJson(Stack stack) {
        return printer.toJson(stack);
    }

    public Map<String, Object> sanitiseTemplate(Map<String, Object> map) {
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

        if (Objects.equals("Manifest", entry.getKey())
                && entry.getValue() instanceof Map map
                && map.containsKey("Fn::Join")) {
            return Map.entry(entry.getKey(), "manifest-with-refs-scrubbed-for-test");
        }

        if (Objects.equals("Manifest", entry.getKey()) && entry.getValue() instanceof String s) {
            return Map.entry(entry.getKey(), sortJsonString(s));
        }

        if (entry.getValue() instanceof Map map) {
            return Map.entry(entry.getKey(), sanitiseTemplate(map));
        }

        return entry;
    }

    private static String sortJsonString(String s) {
        try {
            Object parsed = new Gson().fromJson(s, Object.class);
            return new Gson().toJson(sortMapsRecursively(parsed));
        } catch (JsonSyntaxException e) {
            return s;
        }
    }

    private static Object sortMapsRecursively(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((k, v) -> sorted.put(k.toString(), sortMapsRecursively(v)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(SleeperInstancePrinter::sortMapsRecursively).toList();
        }
        return value;
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
}
