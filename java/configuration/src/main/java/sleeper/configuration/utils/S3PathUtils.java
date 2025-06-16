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
        List<String> outList = new ArrayList<String>();
        files.stream().forEach(file -> {
            outList.addAll(listFilesAsS3Objects(file)
                    .stream().map(S3Object::key)
                    .collect(Collectors.toList()));
        });
        return outList.stream();
    }

    /**
     * Streams S3Objects for files from S3 for list of paths provided.
     *
     * @param  files list of file paths to expand
     * @return       stream of files as S3Objects found at given paths
     */
    public Stream<S3Object> streamFilesAsS3Objects(List<String> files) {
        List<S3Object> outList = new ArrayList<S3Object>();
        files.stream().forEach(file -> {
            outList.addAll(listFilesAsS3Objects(file));
        });
        return outList.stream();
    }

    /**
     * Lists the S3Object details of file for singular path provided.
     *
     * @param  filename name of file
     * @return          s3Object containing all files details
     */
    private List<S3Object> listFilesAsS3Objects(String filename) {
        String bucket = filename.substring(0, filename.indexOf("/"));
        String objectKey = filename.substring(filename.indexOf("/") + 1);
        List<S3Object> outList = new ArrayList<S3Object>();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(objectKey)
                .build());

        for (ListObjectsV2Response subResponse : response) {
            subResponse.contents().forEach((S3Object s3Object) -> {
                if (!s3Object.key().contains(".crc")) {
                    outList.add(s3Object);
                }
            });
        }

        return outList;
    }
}
