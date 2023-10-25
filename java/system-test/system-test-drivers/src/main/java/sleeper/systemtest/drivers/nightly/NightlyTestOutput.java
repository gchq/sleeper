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
    private static final PathMatcher SITE_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**-site.zip");

    private final List<TestResult> tests;

    public NightlyTestOutput(List<TestResult> tests) {
        this.tests = Objects.requireNonNull(tests, "tests must not be null");
    }

    public void uploadToS3(AmazonS3 s3Client, String bucketName, NightlyTestTimestamp timestamp) {
        NightlyTestUploader.builder()
                .s3Client(s3Client)
                .bucketName(bucketName)
                .timestamp(timestamp)
                .build().upload(this);
    }

    public Stream<Path> filesToUpload() {
        return tests.stream().flatMap(TestResult::filesToUpload);
    }

    public List<TestResult> getTests() {
        return tests;
    }

    public static NightlyTestOutput from(Path directory) throws IOException {
        List<Path> logFiles = new ArrayList<>();
        List<Path> statusFiles = new ArrayList<>();
        List<Path> siteFiles = new ArrayList<>();
        forEachFileIn(directory, file -> {
            if (LOG_FILE_MATCHER.matches(file)) {
                logFiles.add(file);
            } else if (STATUS_FILE_MATCHER.matches(file)) {
                statusFiles.add(file);
            } else if (SITE_FILE_MATCHER.matches(file)) {
                siteFiles.add(file);
            }
        });
        return fromLogAndStatusFiles(directory, logFiles, statusFiles, siteFiles);
    }

    private static void forEachFileIn(Path directory, Consumer<Path> action) throws IOException {
        try (Stream<Path> entriesInDirectory = Files.list(directory)) {
            entriesInDirectory.filter(Files::isRegularFile).forEach(action);
        }
    }

    private static NightlyTestOutput fromLogAndStatusFiles(
            Path directory, List<Path> logFiles, List<Path> statusFiles, List<Path> siteFiles) throws IOException {
        Map<String, TestResult.Builder> resultByTestName = new HashMap<>();
        for (Path logFile : logFiles) {
            getResultBuilder(readTestName(logFile), resultByTestName)
                    .logFile(logFile);
        }
        for (Path statusFile : statusFiles) {
            readStatusFile(statusFile, getResultBuilder(readTestName(statusFile), resultByTestName));
        }
        for (Path siteFile : siteFiles) {
            getResultBuilder(readSiteTestName(siteFile), resultByTestName)
                    .siteFile(siteFile);
        }
        loadReportFiles(directory, resultByTestName);
        return new NightlyTestOutput(resultByTestName.values().stream()
                .map(TestResult.Builder::build)
                .sorted(Comparator.comparing(TestResult::getTestName))
                .collect(Collectors.toList()));
    }

    private static void loadReportFiles(Path directory, Map<String, TestResult.Builder> resultByTestName) throws IOException {
        for (Map.Entry<String, TestResult.Builder> testEntry : resultByTestName.entrySet()) {
            Path testDirectory = directory.resolve(testEntry.getKey());
            if (!Files.isDirectory(testDirectory)) {
                continue;
            }
            TestResult.Builder result = testEntry.getValue();
            try (Stream<Path> files = Files.list(testDirectory)) {
                files.filter(LOG_FILE_MATCHER::matches)
                        .forEach(result::logFile);
            }
        }
    }

    private static TestResult.Builder getResultBuilder(
            String testName, Map<String, TestResult.Builder> resultByTestName) {
        return resultByTestName.computeIfAbsent(testName,
                name -> TestResult.builder().testName(name));
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

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private static String readSiteTestName(Path file) {
        String fullFilename = file.getFileName().toString();
        return fullFilename.substring(0, fullFilename.lastIndexOf('-'));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        NightlyTestOutput that = (NightlyTestOutput) object;
        return Objects.equals(tests, that.tests);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tests);
    }

    @Override
    public String toString() {
        return "NightlyTestOutput{" +
                "tests=" + tests +
                '}';
    }
}
