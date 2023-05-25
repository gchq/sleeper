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
package sleeper.systemtest.nightly;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class TestResult {
    private final String testName;
    private final int exitCode;
    private final String instanceId;
    private final List<Path> logFiles;

    private TestResult(Builder builder) {
        testName = builder.testName;
        exitCode = builder.exitCode;
        instanceId = builder.instanceId;
        logFiles = builder.logFiles;
        Collections.sort(logFiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Stream<Path> streamLogFiles() {
        return logFiles.stream();
    }

    public String getTestName() {
        return testName;
    }

    public int getExitCode() {
        return exitCode;
    }

    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestResult that = (TestResult) o;

        if (exitCode != that.exitCode) {
            return false;
        }
        if (!Objects.equals(testName, that.testName)) {
            return false;
        }
        if (!Objects.equals(instanceId, that.instanceId)) {
            return false;
        }
        return logFiles.equals(that.logFiles);
    }

    @Override
    public int hashCode() {
        int result = testName != null ? testName.hashCode() : 0;
        result = 31 * result + exitCode;
        result = 31 * result + (instanceId != null ? instanceId.hashCode() : 0);
        result = 31 * result + logFiles.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TestResult{" +
                "testName='" + testName + '\'' +
                ", exitCode=" + exitCode +
                ", instanceId='" + instanceId + '\'' +
                ", logFiles=" + logFiles +
                '}';
    }

    public static final class Builder {
        private String testName;
        private int exitCode;
        private String instanceId;
        private final List<Path> logFiles = new ArrayList<>();

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder testName(String testName) {
            this.testName = testName;
            return this;
        }

        public Builder exitCode(int exitCode) {
            this.exitCode = exitCode;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder logFile(Path logFile) {
            logFiles.add(logFile);
            return this;
        }

        public TestResult build() {
            return new TestResult(this);
        }
    }
}
