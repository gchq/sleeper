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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
                .map(S3Location::parse)
                .map(this::findContents)
                .toList());
    }

    private S3PathContents findContents(S3Location location) {
        return new S3PathContents(location, listFilesAsS3FileDetails(location));
    }

    private List<S3FileDetails> listFilesAsS3FileDetails(S3Location location) {

        List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(location.bucket)
                .prefix(location.prefix)
                .build());

        for (ListObjectsV2Response subResponse : response) {
            subResponse.contents().forEach((S3Object s3Object) -> {
                if (checkIsNotCrcFile(s3Object.key())) {
                    outList.add(new S3FileDetails(location.bucket, s3Object));
                }
            });
        }

        return outList;
    }

    private static boolean checkIsNotCrcFile(String key) {
        return !key.endsWith(".crc");
    }

    public record S3ExpandDirectoriesResult(List<S3PathContents> contents) {

        public List<String> listHadoopPathsThrowIfAnyPathIsEmpty() {
            return streamFilesThrowIfAnyPathIsEmpty()
                    .map(S3FileDetails::pathForHadoop)
                    .toList();
        }

        public List<String> listJobPathsThrowIfAnyPathIsEmpty() {
            return streamFilesThrowIfAnyPathIsEmpty()
                    .map(S3FileDetails::pathForJob)
                    .toList();
        }

        public Stream<S3FileDetails> streamFilesThrowIfAnyPathIsEmpty() {
            return contents.stream()
                    .peek(S3PathContents::throwIfEmpty)
                    .flatMap(path -> path.contents().stream());
        }
    }

    /**
     * Results of expanding a path in S3.
     *
     * @param location the location in S3
     * @param contents the files found
     */
    public record S3PathContents(S3Location location, List<S3FileDetails> contents) {

        public void throwIfEmpty() throws S3FileNotFoundException {
            if (contents.isEmpty()) {
                throw new S3FileNotFoundException(location.bucket, location.prefix);
            }
        }
    }

    /**
     * A file found in an S3 bucket.
     *
     * @param bucket   the S3 bucket name
     * @param s3Object details of the S3 object
     */
    public record S3FileDetails(String bucket, S3Object s3Object) {

        public String pathForJob() {
            return bucket() + "/" + objectKey();
        }

        public String pathForHadoop() {
            return "s3a://" + bucket() + "/" + objectKey();
        }

        public String objectKey() {
            return s3Object.key();
        }

        public long fileSizeBytes() {
            return s3Object.size();
        }
    }

    /**
     * A location in S3 to look for files.
     *
     * @param requestPath the path requested before parsing
     * @param bucket      the S3 bucket name
     * @param prefix      prefix for object keys
     */
    public record S3Location(String requestPath, String bucket, String prefix) {

        /**
         * Parses a path from a request in an ingest job, bulk import job or ingest batcher submission.
         *
         * @param  path the path
         * @return      the parsed location in S3
         */
        public static S3Location parse(String path) {
            if (!path.contains("/")) {
                return new S3Location(path, path, "");
            } else {
                return new S3Location(path,
                        path.substring(0, path.indexOf("/")),
                        path.substring(path.indexOf("/") + 1));
            }
        }
    }
}
