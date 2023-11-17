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
package sleeper.systemtest.drivers.nightly;

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
    private final List<Path> rootFiles;
    private final List<Path> nestedFiles;

    private TestResult(Builder builder) {
        testName = Objects.requireNonNull(builder.testName, "testName must not be null");
        exitCode = builder.exitCode;
        instanceId = builder.instanceId;
        rootFiles = Objects.requireNonNull(builder.rootFiles, "rootFiles must not be null");
        nestedFiles = Objects.requireNonNull(builder.nestedFiles, "nestedFiles must not be null");
        Collections.sort(rootFiles);
        Collections.sort(nestedFiles);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Stream<NightlyTestUploadFile> uploads() {
        return Stream.concat(rootUploads(), nestedUploads());
    }

    private Stream<NightlyTestUploadFile> rootUploads() {
        return rootFiles.stream()
                .map(NightlyTestUploadFile::fileInUploadDir);
    }

    private Stream<NightlyTestUploadFile> nestedUploads() {
        return nestedFiles.stream()
                .map(file -> NightlyTestUploadFile.fileInS3RelativeDir(testName, file));
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
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TestResult that = (TestResult) object;
        return exitCode == that.exitCode
                && Objects.equals(testName, that.testName)
                && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(rootFiles, that.rootFiles)
                && Objects.equals(nestedFiles, that.nestedFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testName, exitCode, instanceId, rootFiles, nestedFiles);
    }

    @Override
    public String toString() {
        return "TestResult{" +
                "testName='" + testName + '\'' +
                ", exitCode=" + exitCode +
                ", instanceId='" + instanceId + '\'' +
                ", rootFiles=" + rootFiles +
                ", nestedFiles=" + nestedFiles +
                '}';
    }

    public static final class Builder {
        private String testName;
        private int exitCode = 1;
        private String instanceId;
        private final List<Path> rootFiles = new ArrayList<>();
        private final List<Path> nestedFiles = new ArrayList<>();

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

        public Builder siteFile(Path siteFile) {
            rootFiles.add(siteFile);
            return this;
        }

        public Builder logFile(Path logFile) {
            rootFiles.add(logFile);
            return this;
        }

        public Builder outputFiles(List<Path> outputFiles) {
            nestedFiles.addAll(outputFiles);
            return this;
        }

        public TestResult build() {
            return new TestResult(this);
        }
    }
}
