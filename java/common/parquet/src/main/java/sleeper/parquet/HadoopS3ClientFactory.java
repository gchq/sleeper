/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.IOException;
import java.net.URI;

public class HadoopS3ClientFactory extends Configured implements S3ClientFactory {

    public HadoopS3ClientFactory() {
    }

    public static void configureHadoop(Configuration conf) {
        conf.set("fs.s3a.s3.client.factory.impl", HadoopS3ClientFactory.class.getName());
    }

    @Override
    public S3Client createS3Client(URI uri, S3ClientCreationParameters params) throws IOException {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(params.getCredentialSet())
                .forcePathStyle(params.isPathStyleAccess());
        String region = "eu-west-2";//params.getRegion();
        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }
        String endpoint = params.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    @Override
    public S3AsyncClient createS3AsyncClient(URI uri, S3ClientCreationParameters params) throws IOException {
        S3AsyncClientBuilder builder = S3AsyncClient.builder()
                .credentialsProvider(params.getCredentialSet())
                .forcePathStyle(params.isPathStyleAccess());
        String region = params.getRegion();
        if (region != null && !region.isEmpty()) {
            builder.region(Region.of(region));
        }
        String endpoint = params.getEndpoint();
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    @Override
    public S3TransferManager createS3TransferManager(S3AsyncClient asyncClient) {
        return S3TransferManager.builder().s3Client(asyncClient).build();
    }

}
