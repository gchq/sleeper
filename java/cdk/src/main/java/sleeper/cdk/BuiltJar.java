/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.cdk;

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.S3Code;
import software.amazon.awscdk.services.lambda.VersionOptions;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

public class BuiltJar {

    public static final Jar ATHENA = new Jar("athena-%s.jar");
    public static final Jar BULK_IMPORT_STARTER = new Jar("bulk-import-starter-%s.jar");
    public static final Jar INGEST_STARTER = new Jar("ingest-starter-%s.jar");
    public static final Jar GARBAGE_COLLECTOR = new Jar("lambda-garbagecollector-%s.jar");
    public static final Jar COMPACTION_JOB_CREATOR = new Jar("lambda-jobSpecCreationLambda-%s.jar");
    public static final Jar COMPACTION_TASK_CREATOR = new Jar("runningjobs-%s.jar");
    public static final Jar PARTITION_SPLITTER = new Jar("lambda-splitter-%s.jar");
    public static final Jar QUERY = new Jar("query-%s.jar");
    public static final Jar CUSTOM_RESOURCES = new Jar("cdk-custom-resources-%s.jar");
    public static final Jar METRICS = new Jar("metrics-%s.jar");

    private final Context context;
    private final Jar jar;

    public BuiltJar(Context context, Jar jar) {
        this.context = context;
        this.jar = jar;
    }

    public static Context withContext(Construct scope, InstanceProperties properties) {
        return withPropertiesAndDirectory(properties,
                Path.of((String) scope.getNode().tryGetContext("jarsdir")));
    }

    public static Context withPropertiesAndDirectory(InstanceProperties properties, Path jarsDirectory) {
        return new Context(properties, jarsDirectory);
    }

    public LambdaCode lambdaCodeFrom(IBucket jarsBucket) {
        try {
            return new LambdaCode(jarsBucket, fileName(), codeSha256());
        } catch (NoSuchAlgorithmException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String fileName() {
        return String.format(jar.jarFormat, context.version());
    }

    public String codeSha256() throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream is = Files.newInputStream(context.jarsDirectory.resolve(fileName()));
             DigestInputStream digestStream = new DigestInputStream(is, digest)) {
            while (digestStream.read() != -1) {
                // Consume stream
            }
        }
        return Base64.getEncoder().encodeToString(digest.digest());
    }

    public static final class LambdaCode {
        private final S3Code code;
        private final VersionOptions versionOptions;

        public LambdaCode(IBucket jarsBucket, String filename, String codeSha256) {
            this.code = Code.fromBucket(jarsBucket, filename);
            this.versionOptions = VersionOptions.builder()
                    .codeSha256(codeSha256)
                    .build();
        }

        public S3Code code() {
            return code;
        }

        public VersionOptions versionOptions() {
            return versionOptions;
        }
    }

    public static final class Context {
        private final InstanceProperties properties;
        private final Path jarsDirectory;

        private Context(InstanceProperties properties, Path jarsDirectory) {
            this.properties = properties;
            this.jarsDirectory = jarsDirectory;
        }

        public String version() {
            return properties.get(VERSION);
        }

        public BuiltJar jar(Jar jar) {
            return new BuiltJar(this, jar);
        }
    }

    public static final class Jar {
        private final String jarFormat;

        public Jar(String jarFormat) {
            this.jarFormat = jarFormat;
        }
    }

}
