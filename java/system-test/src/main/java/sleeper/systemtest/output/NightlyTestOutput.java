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
package sleeper.systemtest.output;

import com.amazonaws.services.s3.AmazonS3;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class NightlyTestOutput {

    private static final PathMatcher LOG_FILE_MATCHER = FileSystems.getDefault().getPathMatcher("glob:**.log");

    private final List<Path> logFiles;

    private NightlyTestOutput(List<Path> logFiles) {
        this.logFiles = logFiles;
    }

    public static NightlyTestOutput from(Path directory) throws IOException {
        List<Path> logFiles = new ArrayList<>();
        try (Stream<Path> entriesInDirectory = Files.list(directory)) {
            entriesInDirectory.filter(LOG_FILE_MATCHER::matches)
                    .filter(Files::isRegularFile)
                    .forEach(logFiles::add);
        }
        return new NightlyTestOutput(logFiles);
    }

    public void uploadToS3(AmazonS3 s3Client, String bucketName, NightlyTestTimestamp timestamp) {
        logFiles.forEach(path -> s3Client.putObject(bucketName,
                getPathInS3(timestamp, path),
                path.toFile()));
    }

    private static String getPathInS3(NightlyTestTimestamp timestamp, Path filePath) {
        return timestamp.getS3FolderName() + "/" + filePath.getFileName();
    }
}
