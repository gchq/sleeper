/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.environment.cdk.outputs;

import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.Output;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class StackOutputs {

    private final List<Stack> stacks;

    private StackOutputs(List<Stack> stacks) {
        this.stacks = stacks;
    }

    public static StackOutputs load(CloudFormationClient cloudFormation, List<String> stackNames) {
        return new StackOutputs(stackNames.stream()
                .flatMap(stackName -> loadStack(cloudFormation, stackName).stream())
                .collect(Collectors.toUnmodifiableList()));
    }

    public static StackOutputs fromMap(Map<String, Map<String, String>> outputsByStackName) {
        return new StackOutputs(outputsByStackName.entrySet().stream()
                .map(entry -> new Stack(entry.getKey(), entry.getValue()))
                .collect(Collectors.toUnmodifiableList()));
    }

    private static Optional<Stack> loadStack(CloudFormationClient cloudFormation, String stackName) {
        return cloudFormation.describeStacks(builder -> builder.stackName(stackName))
                .stacks().stream().findFirst()
                .map(stack -> loadOutputs(stack.outputs()))
                .map(outputs -> new Stack(stackName, outputs));
    }

    private static Map<String, String> loadOutputs(List<Output> outputs) {
        return outputs.stream().collect(
                Collectors.toMap(Output::outputKey, Output::outputValue));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StackOutputs that = (StackOutputs) o;
        return Objects.equals(stacks, that.stacks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stacks);
    }

    @Override
    public String toString() {
        return "StackOutputs{" +
                "stacks=" + stacks +
                '}';
    }

    public static class Stack {
        private final String stackName;
        private final Map<String, String> outputs;

        private Stack(String stackName, Map<String, String> outputs) {
            this.stackName = stackName;
            this.outputs = outputs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Stack stack = (Stack) o;
            return Objects.equals(stackName, stack.stackName) && Objects.equals(outputs, stack.outputs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stackName, outputs);
        }

        @Override
        public String toString() {
            return "Stack{" +
                    "stackName='" + stackName + '\'' +
                    ", outputs=" + outputs +
                    '}';
        }
    }
}
