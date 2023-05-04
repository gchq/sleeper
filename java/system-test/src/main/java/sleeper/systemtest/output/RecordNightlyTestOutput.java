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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

public class RecordNightlyTestOutput {

    private RecordNightlyTestOutput() {
    }

    private static final DateTimeFormatter S3_PREFIX_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .withZone(ZoneId.of("UTC"));

    public static void uploadLogFiles(AmazonS3 s3Client, String bucketName, long timestamp, Path output) throws IOException {
        Instant startTime = parseCommandLineTimestamp(timestamp);
        try (Stream<Path> entriesInDirectory = Files.list(output)) {
            entriesInDirectory.forEach(path ->
                    s3Client.putObject(bucketName,
                            getPathInS3(startTime, path),
                            path.toFile()));
        }
    }

    public static String getPathInS3(Instant startTime, Path filePath) {
        return S3_PREFIX_FORMAT.format(startTime) + "/" + filePath.getFileName();
    }

    public static Instant parseCommandLineTimestamp(long timestamp) {
        return Instant.ofEpochSecond(timestamp);
    }
}
