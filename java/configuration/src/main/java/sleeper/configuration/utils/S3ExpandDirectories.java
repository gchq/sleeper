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
 * Hadoop s3a:// paths, or <bucket-name>/object-key>.
 */
public class S3ExpandDirectories {

    private final S3Client s3Client;

    public S3ExpandDirectories(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Discovers files under the given paths. Works with paths in the format for an ingest job.
     *
     * @param  filenames filenames to expand, in the format <bucket-name>/<object-key>
     * @return           filenames found, in the format <bucket-name>/<object-key>
     */
    public List<String> expandJobFilenames(List<String> filenames) {
        return streamFilesAsS3FileDetails(filenames)
                .map(S3FileDetails::pathForJob)
                .toList();
    }

    /**
     * Discovers files under the given paths. Takes paths in the format for an ingest job, and returns Hadoop paths.
     *
     * @param  filenames filenames to expand, in the format <bucket-name>/<object-key>
     * @return           filenames found, in the format s3a://<bucket-name>/<object-key>
     */
    public List<String> expandJobFilenamesForHadoop(List<String> filenames) {
        return streamFilesAsS3FileDetails(filenames)
                .map(S3FileDetails::hadoopPath)
                .toList();
    }

    /**
     * Discovers files under the given paths. Takes paths in the format for an ingest job, and returns details of each
     * file.
     *
     * @param  filenames filenames to expand, in the format <bucket-name>/<object-key>
     * @return           stream of files found at given paths
     */
    public Stream<S3FileDetails> streamFilesAsS3FileDetails(List<String> filenames) {
        if ((filenames == null) || filenames.isEmpty()) {
            return Stream.empty();
        }
        List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
        filenames.stream().forEach(file -> {
            outList.addAll(listFilesAsS3FileDetails(file));
        });
        return outList.stream();
    }

    private List<S3FileDetails> listFilesAsS3FileDetails(String filename) {
        if (checkIsNotCrcFile(filename)) {
            FileLocationDetails fileLocation = determineFileLocationBreakdown(filename);

            List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
            ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                    .bucket(fileLocation.bucket)
                    .prefix(fileLocation.objectKey)
                    .build());

            for (ListObjectsV2Response subResponse : response) {
                subResponse.contents().forEach((S3Object s3Object) -> {
                    if (checkIsNotCrcFile(s3Object.key())) {
                        outList.add(new S3FileDetails(fileLocation.bucket, s3Object));
                    }
                });
            }
            if (outList.isEmpty()) {
                throw new S3FileNotFoundException(fileLocation.bucket, fileLocation.objectKey);
            }

            return outList;
        } else {
            return List.of();
        }

    }

    private FileLocationDetails determineFileLocationBreakdown(String filename) {
        if (!filename.contains("/")) {
            return new FileLocationDetails(filename, "");
        } else {
            return new FileLocationDetails(filename.substring(0, filename.indexOf("/")),
                    filename.substring(filename.indexOf("/") + 1));
        }
    }

    private boolean checkIsNotCrcFile(String key) {
        return !key.contains(".crc");
    }

    /**
     * Storage of the details from with s3 with full file name.
     *
     * @param fileLocation name of file including bucket
     * @param fileObject   respresentation of object back from s3
     */
    public record S3FileDetails(String bucket, S3Object fileObject) {

        public String pathForJob() {
            return bucket() + "/" + objectKey();
        }

        public String hadoopPath() {
            return "s3a://" + bucket() + "/" + objectKey();
        }

        public String objectKey() {
            return fileObject.key();
        }

        public long fileSizeBytes() {
            return fileObject.size();
        }
    }

    /**
     * Storage for the breadown of the filename into the relevant parts.
     *
     * @param bucket    bucket location of file
     * @param objectKey name of file
     */
    public record FileLocationDetails(String bucket, String objectKey) {
    }
}
