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

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Class for retrieve paths for S3. */
public class S3PathUtils {

    private final S3Client s3Client;

    public S3PathUtils(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Streams filenames back from bucket for list of paths provided.
     *
     * @param  bucket                s3 bucket to retrieve from
     * @param  files                 list of file paths to expand
     * @return                       list of full filenames of all paths detailed
     * @throws FileNotFoundException thrown when path not valid within s3
     */
    public List<String> streamFileKeyByPath(String bucket, List<String> files) throws FileNotFoundException {
        List<String> outList = new ArrayList<>();
        files.stream().forEach(file -> {
            try {
                outList.addAll(streamFileKeyByPath(bucket, file));
            } catch (FileNotFoundException e) {
            }
        });
        return outList;
    }

    /**
     * Streams filenames back from bucket for singular path provided
     *
     * @param  bucket                s3 bucket to retrieve from
     * @param  path                  path of file to expand
     * @return                       list of full filenames at path
     * @throws FileNotFoundException thrown when path not valid within s3
     */
    public List<String> streamFileKeyByPath(String bucket, String path) throws FileNotFoundException {
        return streamFileDetails(bucket, path)
                .stream().map(S3FileDetails::filename)
                .collect(Collectors.toList());
    }

    /**
     * Streams file details back from bucket for singular path provided
     *
     * @param  bucket                s3 bucket to retrieve from
     * @param  path                  path of file to expand
     * @return                       details of files contained at paths
     * @throws FileNotFoundException thrown when path not valid within s3
     */
    private List<S3FileDetails> streamFileDetails(String bucket, String path) throws FileNotFoundException {
        List<S3FileDetails> outList = new ArrayList<S3FileDetails>();
        ListObjectsV2Iterable response = s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(path)
                .build());

        for (ListObjectsV2Response subResponse : response) {
            subResponse.contents().forEach((S3Object s3Object) -> {
                outList.add(generateS3FileDetails(bucket, s3Object));
            });
        }

        if (outList.isEmpty()) {
            throw new FileNotFoundException();
        }

        return outList;
    }

    /**
     * Builds record object of key file details retrieved from S3.
     *
     * @param  bucket bucket with file stored
     * @param  object object return from s3
     * @return        record of key details
     */
    private S3FileDetails generateS3FileDetails(String bucket, S3Object object) {
        return new S3FileDetails(bucket + "/" + object.key(), object.size());
    }

    /** Record class for import file information. */
    public record S3FileDetails(String filename, long fileSizeBytes) {
    }
}
