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
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.assertions.Template;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toMap;

public class StackPrinter {
    private final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();
    private final UnaryOperator<Map<String, Object>> sanitiser;

    public StackPrinter(UnaryOperator<Map<String, Object>> sanitiser) {
        this.sanitiser = sanitiser;
    }

    public String toJson(Stack stack) {
        List<Stack> stacks = stack.getNode().findAll().stream()
                .filter(construct -> construct instanceof Stack)
                .map(construct -> (Stack) construct)
                .toList();
        Map<String, Object> stackIdToTemplate = stacks.stream()
                .collect(toMap(
                        s -> s.getNode().getId(),
                        s -> toSanitisedMap(s),
                        (stack1, stack2) -> {
                            throw new IllegalStateException("Duplicate stack ID");
                        },
                        LinkedHashMap::new));
        return gson.toJson(Map.of("StackIdToTemplate", stackIdToTemplate));
    }

    private Map<String, Object> toSanitisedMap(Stack stack) {
        Map<String, Object> map = Template.fromStack(stack).toJSON();
        return sanitiser.apply(map);
    }

}
