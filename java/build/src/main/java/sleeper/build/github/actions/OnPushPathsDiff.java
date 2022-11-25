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
package sleeper.build.github.actions;

import sleeper.build.chunks.ProjectChunk;
import sleeper.build.chunks.ProjectStructure;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OnPushPathsDiff {

    private final List<String> expected;
    private final List<String> actual;
    private final List<String> missingEntries;
    private final List<String> extraEntries;

    private OnPushPathsDiff(Builder builder) {
        expected = Objects.requireNonNull(builder.expected, "expected must not be null");
        actual = Objects.requireNonNull(builder.actual, "actual must not be null");
        missingEntries = Objects.requireNonNull(builder.missingEntries, "missingEntries must not be null");
        extraEntries = Objects.requireNonNull(builder.extraEntries, "extraEntries must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static OnPushPathsDiff fromExpectedAndActual(List<String> expected, List<String> actual) {
        List<String> missingEntries = new ArrayList<>(expected);
        missingEntries.removeAll(actual);
        List<String> extraEntries = new ArrayList<>(actual);
        extraEntries.removeAll(expected);
        return builder()
                .expected(expected).actual(actual)
                .missingEntries(missingEntries).extraEntries(extraEntries)
                .build();
    }

    public void reportMissingEntries(PrintStream out, ProjectStructure project, ProjectChunk chunk) {
        if (!missingEntries.isEmpty()) {
            out.println("Found missing on.push.paths at " + project.workflowPathInRepository(chunk) + ":");
            missingEntries.forEach(out::println);
            out.println();
            out.println("Expected entries:");
            expected.forEach(entry -> out.println("- '" + entry + "'"));
            out.println();
        }
    }

    public void reportExtraEntries(ProjectStructure project, ProjectChunk chunk, PrintStream out) {
        if (!extraEntries.isEmpty()) {
            out.println("Found extra on.push.paths at " + project.workflowPathInRepository(chunk) + ":");
            extraEntries.forEach(out::println);
            out.println();
        }
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
        OnPushPathsDiff that = (OnPushPathsDiff) o;
        return expected.equals(that.expected) && actual.equals(that.actual) && missingEntries.equals(that.missingEntries) && extraEntries.equals(that.extraEntries);
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

        public OnPushPathsDiff build() {
            return new OnPushPathsDiff(this);
        }
    }
}
