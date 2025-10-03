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
package sleeper.clients.deploy.jar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static sleeper.clients.util.BucketUtils.doesBucketExist;
import static sleeper.clients.util.ClientUtils.optionalArgument;

public class SyncJars {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncJars.class);
    private final S3Client s3;
    private final Path jarsDirectory;

    public SyncJars(S3Client s3, Path jarsDirectory) {
        this.s3 = requireNonNull(s3, "s3 must not be null");
        this.jarsDirectory = requireNonNull(jarsDirectory, "jarsDirectory must not be null");
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Usage: <jars-dir> <bucket-name> <region> <optional-delete-old-jars>");
        }
        Path jarsDirectory = Path.of(args[0]);
        String bucketName = args[1];
        String region = args[2];
        boolean deleteOldJars = optionalArgument(args, 3)
                .map(Boolean::parseBoolean)
                .orElse(false);
        try (S3Client s3 = S3Client.create()) {
            new SyncJars(s3, jarsDirectory)
                    .sync(SyncJarsRequest.builder()
                            .bucketName(bucketName)
                            .region(region)
                            .deleteOldJars(deleteOldJars)
                            .build());
        }
    }

    public boolean sync(SyncJarsRequest request) throws IOException {
        String bucketName = request.getBucketName();
        // Note that LocalStack doesn't fail bucket creation if it already exists, but the AWS API does.
        boolean changed = false;
        if (!doesBucketExist(s3, bucketName)) {
            changed = true;

            LOGGER.info("Creating jars bucket: {}", bucketName);
            s3.createBucket(builder -> builder
                    .bucket(bucketName)
                    .acl(BucketCannedACL.PRIVATE)
                    .createBucketConfiguration(configBuilder -> configBuilder
                            .locationConstraint(bucketLocationConstraint(request.getRegion()))));
            s3.putPublicAccessBlock(builder -> builder
                    .bucket(bucketName)
                    .publicAccessBlockConfiguration(configBuilder -> configBuilder
                            .blockPublicAcls(true)
                            .ignorePublicAcls(true)
                            .blockPublicPolicy(true)
                            .restrictPublicBuckets(true)));

            // We enable versioning so that the CDK is able to update the functions when the code changes in the bucket.
            // See the following:
            // https://www.define.run/posts/cdk-not-updating-lambda/
            // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
            // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
            s3.putBucketVersioning(builder -> builder
                    .bucket(bucketName)
                    .versioningConfiguration(config -> config.status(BucketVersioningStatus.ENABLED)));
        }

        List<Path> jars = listJarsInDirectory(jarsDirectory);
        LOGGER.info("Found {} jars in local directory", jars.size());

        JarsDiff diff = JarsDiff.from(jarsDirectory, jars,
                s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName)));
        Collection<Path> uploadJars = diff.getModifiedAndNew().stream()
                .filter(request.getUploadFilter())
                .collect(Collectors.toUnmodifiableList());
        Collection<String> deleteKeys = diff.getS3KeysToDelete();

        if (request.isDeleteOldJars() && !deleteKeys.isEmpty()) {
            LOGGER.info("Deleting {} jars from bucket", deleteKeys.size());
            s3.deleteObjects(builder -> builder
                    .bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder
                            .objects(deleteKeys.stream()
                                    .map(deleteKey -> ObjectIdentifier.builder().key(deleteKey).build())
                                    .collect(Collectors.toList()))));
            changed = true;
        }

        LOGGER.info("Uploading {} jars", uploadJars.size());
        uploadJars.stream().parallel().forEach(jar -> {
            LOGGER.info("Uploading jar: {}", jar.getFileName());
            s3.putObject(builder -> builder
                    .bucket(bucketName)
                    .key(String.valueOf(jar.getFileName())),
                    jar);
            LOGGER.info("Finished uploading jar: {}", jar.getFileName());
        });
        if (!uploadJars.isEmpty()) {
            changed = true;
        }
        return changed;
    }

    private static List<Path> listJarsInDirectory(Path directory) throws IOException {
        try (Stream<Path> jars = Files.list(directory)) {
            return jars.filter(path -> path.toFile().getName().endsWith(".jar")).collect(Collectors.toList());
        }
    }

    private static BucketLocationConstraint bucketLocationConstraint(String region) {
        // The us-east-1 region is returned as UNKNOWN_TO_SDK_VERSION, which incorrectly serialises as a string "null".
        BucketLocationConstraint constraint = BucketLocationConstraint.fromValue(region);
        if (constraint == BucketLocationConstraint.UNKNOWN_TO_SDK_VERSION) {
            return null;
        } else {
            return constraint;
        }
    }
}
