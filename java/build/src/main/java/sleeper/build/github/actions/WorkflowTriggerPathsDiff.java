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
package sleeper.build.github.actions;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;

import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class WorkflowTriggerPathsDiff {

    private final List<String> expected;
    private final List<String> actual;
    private final List<String> missingEntries;
    private final List<String> extraEntries;

    private WorkflowTriggerPathsDiff(Builder builder) {
        expected = Objects.requireNonNull(builder.expected, "expected must not be null");
        actual = Objects.requireNonNull(builder.actual, "actual must not be null");
        missingEntries = Objects.requireNonNull(builder.missingEntries, "missingEntries must not be null");
        extraEntries = Objects.requireNonNull(builder.extraEntries, "extraEntries must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static WorkflowTriggerPathsDiff fromExpectedAndActual(
            ProjectStructure project, List<String> expected, List<String> actual) {
        List<String> missingEntries = expected.stream()
                .filter(entry -> !actual.contains(entry))
                .collect(Collectors.toList());
        List<String> extraEntries = actual.stream()
                .filter(entry -> !expected.contains(entry))
                .filter(project::isUnderMavenPathRepositoryRelative)
                .collect(Collectors.toList());
        return builder()
                .expected(expected).actual(actual)
                .missingEntries(missingEntries).extraEntries(extraEntries)
                .build();
    }

    public void report(PrintStream out, ProjectStructure project, ProjectChunk chunk) {
        if (!missingEntries.isEmpty()) {
            out.println("Found missing trigger paths at " + project.workflowPathInRepository(chunk) + ":");
            missingEntries.forEach(out::println);
            out.println();
            out.println("Expected entries:");
            expected.forEach(entry -> out.println("- '" + entry + "'"));
            out.println();
        }
        if (!extraEntries.isEmpty()) {
            out.println("Found extra trigger paths at " + project.workflowPathInRepository(chunk) + ":");
            extraEntries.forEach(out::println);
            out.println();
        }
    }

    public boolean isValid() {
        return missingEntries.isEmpty() && extraEntries.isEmpty();
    }

    public List<String> getExpected() {
        return expected;
    }

    public List<String> getActual() {
        return actual;
    }

    public List<String> getMissingEntries() {
        return missingEntries;
    }

    public List<String> getExtraEntries() {
        return extraEntries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WorkflowTriggerPathsDiff that = (WorkflowTriggerPathsDiff) o;
        return expected.equals(that.expected) && actual.equals(that.actual)
                && missingEntries.equals(that.missingEntries) && extraEntries.equals(that.extraEntries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expected, actual, missingEntries, extraEntries);
    }

    @Override
    public String toString() {
        return "OnPushPathsDiff{" +
                "expected=" + expected +
                ", actual=" + actual +
                ", missingEntries=" + missingEntries +
                ", extraEntries=" + extraEntries +
                '}';
    }

    public static final class Builder {
        private List<String> expected;
        private List<String> actual;
        private List<String> missingEntries;
        private List<String> extraEntries;

        private Builder() {
        }

        public Builder expected(List<String> expected) {
            this.expected = expected;
            return this;
        }

        public Builder actual(List<String> actual) {
            this.actual = actual;
            return this;
        }

        public Builder missingEntries(List<String> missingEntries) {
            this.missingEntries = missingEntries;
            return this;
        }

        public Builder extraEntries(List<String> extraEntries) {
            this.extraEntries = extraEntries;
            return this;
        }

        public WorkflowTriggerPathsDiff build() {
            return new WorkflowTriggerPathsDiff(this);
        }
    }
}
