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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Class for retrieve paths for S3. */
public class S3PathUtils {

    private final S3Client s3Client;

    public S3PathUtils(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Streams filenames back from S3 for list of paths provided.
     *
     * @param  files list of file paths to expand
     * @return       stream of filenames found at paths
     */
    public Stream<String> streamFilenames(List<String> files) {
        if ((files == null) || files.isEmpty()) {
            return Stream.empty();
        }
        try {
            List<String> outList = new ArrayList<String>();
            files.stream().forEach(file -> {
                outList.addAll(listFilesAsS3FileDetails(file)
                        .stream().map(S3FileDetails::getFullFileLocation)
                        .collect(Collectors.toList()));
            });
            return outList.stream();
        } catch (S3FileNotFoundException e) {
            return Stream.empty();
        }
    }

    /**
     * Streams filenames back from S3 for list of paths provided.
     *
     * @param  files  list of file paths to expand
     * @param  prefix prefix for hadoop path
     * @return        stream of hadoop paths for files found
     */
    public Stream<String> streamHadoopPaths(List<String> files, String prefix) {
        List<String> adjustList = new ArrayList<String>();
        streamFilenames(files).forEach(filename -> adjustList.add(prefix + filename));
        return adjustList.stream();
    }

    /**
     * Streams files from S3 for list of paths provided.
     *
     * @param  files list of file paths to expand
     * @return       stream of files found at given paths
     */
    public Stream<S3FileDetails> streamFilesAsS3FileDetails(List<String> files) {
        if ((files == null) || files.isEmpty()) {
            return Stream.empty();
        }
        try {
            List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
            files.stream().forEach(file -> {
                outList.addAll(listFilesAsS3FileDetails(file));
            });
            return outList.stream();
        } catch (S3FileNotFoundException e) {
            return Stream.empty();
        }
    }

    /**
     * Lists the details of file for singular path provided.
     *
     * @param  filename name of file
     * @return          containing all files details
     */
    public List<S3FileDetails> listFilesAsS3FileDetails(String filename) {
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
                        outList.add(new S3FileDetails(new FileLocationDetails(fileLocation.bucket, s3Object.key()), s3Object));
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
    public record S3FileDetails(FileLocationDetails fileLocation, S3Object fileObject) {

        public String getFullFileLocation() {
            return fileLocation.bucket + "/" + fileLocation().objectKey;
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
