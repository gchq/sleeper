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

import com.amazonaws.services.s3.AmazonS3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NightlyTestOutput {

    private static final PathMatcher LOG_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**.log");
    private static final PathMatcher STATUS_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**.status");

    private final List<TestResult> tests;

    public NightlyTestOutput(List<TestResult> tests) {
        this.tests = Objects.requireNonNull(tests, "tests must not be null");
    }

    public void uploadToS3(AmazonS3 s3Client, String bucketName, NightlyTestTimestamp timestamp) {
        streamLogFiles().forEach(logFile ->
                s3Client.putObject(bucketName,
                        logFilePathInS3(timestamp, logFile),
                        logFile.toFile()));
        NightlyTestSummaryTable.fromS3(s3Client, bucketName)
                .add(timestamp, this)
                .saveToS3(s3Client, bucketName);
    }

    private static String logFilePathInS3(NightlyTestTimestamp timestamp, Path filePath) {
        return timestamp.getS3FolderName() + "/" + filePath.getFileName();
    }

    public Stream<Path> streamLogFiles() {
        return tests.stream()
                .flatMap(TestResult::streamLogFiles);
    }

    public List<TestResult> getTests() {
        return tests;
    }

    public static NightlyTestOutput from(Path directory) throws IOException {
        List<Path> logFiles = new ArrayList<>();
        List<Path> statusFiles = new ArrayList<>();
        forEachFileIn(directory, file -> {
            if (LOG_FILE_MATCHER.matches(file)) {
                logFiles.add(file);
            } else if (STATUS_FILE_MATCHER.matches(file)) {
                statusFiles.add(file);
            }
        });
        return fromLogAndStatusFiles(logFiles, statusFiles);
    }

    private static void forEachFileIn(Path directory, Consumer<Path> action) throws IOException {
        try (Stream<Path> entriesInDirectory = Files.list(directory)) {
            entriesInDirectory.filter(Files::isRegularFile).forEach(action);
        }
    }

    private static NightlyTestOutput fromLogAndStatusFiles(
            List<Path> logFiles, List<Path> statusFiles) throws IOException {
        Map<String, TestResult.Builder> resultByTestName = new HashMap<>();
        for (Path logFile : logFiles) {
            getResultBuilder(logFile, resultByTestName)
                    .logFile(logFile);
        }
        for (Path statusFile : statusFiles) {
            readStatusFile(statusFile, getResultBuilder(statusFile, resultByTestName));
        }
        return new NightlyTestOutput(resultByTestName.values().stream()
                .map(TestResult.Builder::build)
                .sorted(Comparator.comparing(TestResult::getTestName))
                .collect(Collectors.toList()));
    }

    private static TestResult.Builder getResultBuilder(
            Path file, Map<String, TestResult.Builder> resultByTestName) {
        return resultByTestName.computeIfAbsent(
                readTestName(file), testName -> TestResult.builder().testName(testName));
    }

    private static void readStatusFile(Path statusFile, TestResult.Builder builder) throws IOException {
        String[] parts = Files.readString(statusFile).split(" ");
        if (parts.length > 0) {
            builder.exitCode(Integer.parseInt(parts[0]));
        }
        if (parts.length > 1) {
            builder.instanceId(parts[1]);
        }
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private static String readTestName(Path file) {
        String fullFilename = file.getFileName().toString();
        return fullFilename.substring(0, fullFilename.indexOf('.'));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NightlyTestOutput that = (NightlyTestOutput) o;

        return tests.equals(that.tests);
    }

    @Override
    public int hashCode() {
        return tests.hashCode();
    }

    @Override
    public String toString() {
        return "NightlyTestOutput{" +
                "tests=" + tests +
                '}';
    }
}
