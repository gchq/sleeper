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
package sleeper.configuration.utils;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.List;

/**
 * Discovers files under given paths in S3. This is used when submitting files for ingest to a Sleeper table, to allow
 * for submitting directories rather than individual files. It also supports converting to multiple path formats, e.g.
 * Hadoop s3a:// paths, or bucket-name/object-key.
 */
public class S3ExpandDirectories {

    private final S3Client s3Client;

    public S3ExpandDirectories(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Discovers files under the given paths. Takes paths in the format for an ingest job, and returns details of each
     * file.
     *
     * @param  files paths to expand, in the format bucket-name/object-key
     * @return       contents under the specified paths
     */
    public S3ExpandDirectoriesResult expandPaths(List<String> files) {
        if (files == null) {
            return new S3ExpandDirectoriesResult(List.of());
        }
        return new S3ExpandDirectoriesResult(files.stream()
                .filter(S3ExpandDirectories::checkIsNotCrcFile)
                .map(S3Path::parse)
                .map(this::findContents)
                .toList());
    }

    private S3PathContents findContents(S3Path path) {
        return new S3PathContents(path, listFiles(path));
    }

    private List<S3FileDetails> listFiles(S3Path path) {
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(path.bucket())
                .prefix(path.pathInBucket())
                .build());
        return response.contents().stream()
                .filter(s3Object -> checkIsParquetFile(s3Object.key()))
                .map(s3Object -> new S3FileDetails(path.bucket(), s3Object.key(), s3Object.size()))
                .toList();
    }

    private static boolean checkIsNotCrcFile(String key) {
        return !key.endsWith(".crc");
    }

    private static boolean checkIsParquetFile(String key) {
        return key.endsWith(".parquet");
    }
}
