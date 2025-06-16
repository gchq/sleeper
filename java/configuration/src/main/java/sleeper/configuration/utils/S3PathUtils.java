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
        List<String> outList = new ArrayList<String>();
        files.stream().forEach(file -> {
            outList.addAll(listFilesAsS3FileDetails(file)
                    .stream().map(S3FileDetails::filename)
                    .collect(Collectors.toList()));
        });
        return outList.stream();
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
        List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
        files.stream().forEach(file -> {
            outList.addAll(listFilesAsS3FileDetails(file));
        });
        return outList.stream();
    }

    /**
     * Lists the details of file for singular path provided.
     *
     * @param  filename name of file
     * @return          containing all files details
     */
    private List<S3FileDetails> listFilesAsS3FileDetails(String filename) {
        String bucket = filename.substring(0, filename.indexOf("/"));
        String objectKey = filename.substring(filename.indexOf("/") + 1);
        List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(objectKey)
                .build());

        for (ListObjectsV2Response subResponse : response) {
            subResponse.contents().forEach((S3Object s3Object) -> {
                if (isValidFile(s3Object.key())) {
                    outList.add(new S3FileDetails(bucket + "/" + s3Object.key(), s3Object));
                }
            });
        }

        return outList;
    }

    private boolean isValidFile(String key) {
        if (!key.contains(".crc")) {
            return true;
        } else {
            return false;
        }
    }

    public record S3FileDetails(String filename, S3Object fileObject) {
    }
}
