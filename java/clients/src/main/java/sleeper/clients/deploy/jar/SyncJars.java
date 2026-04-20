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
package sleeper.clients.deploy.jar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.sts.StsClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.clients.util.ClientUtils.optionalArgument;

public class SyncJars {
    private static final Logger LOGGER = LoggerFactory.getLogger(SyncJars.class);
    private final S3Client s3;
    private final String accountName;
    private final Path jarsDirectory;

    public SyncJars(S3Client s3, String accountName, Path jarsDirectory) {
        this.s3 = Objects.requireNonNull(s3, "s3 must not be null");
        this.accountName = Objects.requireNonNull(accountName, "accountName must not be null");
        this.jarsDirectory = Objects.requireNonNull(jarsDirectory, "jarsDirectory must not be null");
    }

    public static SyncJars fromScriptsDirectory(S3Client s3, String accountName, Path scriptsDirectory) {
        return new SyncJars(s3, accountName, scriptsDirectory.resolve("jars"));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <jars-dir> <artefacts-deployment-id> <optional-delete-old-jars>");
        }
        Path jarsDirectory = Path.of(args[0]);
        String deploymentId = args[1];
        boolean deleteOldJars = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);
        try (S3Client s3Client = S3Client.create();
                StsClient stsClient = StsClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            new SyncJars(s3Client, accountName, jarsDirectory)
                    .sync(SyncJarsRequest.builder()
                            .deploymentId(deploymentId)
                            .deleteOldJars(deleteOldJars)
                            .build());
        }
    }

    public boolean sync(SyncJarsRequest request) throws IOException {
        String bucketName = request.getBucketNameForAccount(accountName);
        boolean changed = false;

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
}
