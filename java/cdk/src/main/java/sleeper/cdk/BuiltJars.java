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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class BuiltJars {

    public static final Jar ATHENA = new Jar("athena-%s.jar");
    public static final Jar BULK_IMPORT_STARTER = new Jar("bulk-import-starter-%s.jar");
    public static final Jar INGEST_STARTER = new Jar("ingest-starter-%s.jar");
    public static final Jar GARBAGE_COLLECTOR = new Jar("lambda-garbagecollector-%s.jar");
    public static final Jar COMPACTION_JOB_CREATOR = new Jar("lambda-jobSpecCreationLambda-%s.jar");
    public static final Jar COMPACTION_TASK_CREATOR = new Jar("runningjobs-%s.jar");
    public static final Jar PARTITION_SPLITTER = new Jar("lambda-splitter-%s.jar");
    public static final Jar QUERY = new Jar("query-%s.jar");

    private final String version;
    private final Path jarsDirectory;

    private BuiltJars(String version, Path jarsDirectory) {
        this.version = version;
        this.jarsDirectory = jarsDirectory;
    }

    public static BuiltJars withVersionAndPath(String version, Path jarsDirectory) {
        return new BuiltJars(version, jarsDirectory);
    }

    public BuiltJar jar(Jar jar) {
        return new BuiltJar(jar);
    }

    public final class BuiltJar {
        private final Jar jar;

        public BuiltJar(Jar jar) {
            this.jar = jar;
        }

        public String fileName() {
            return String.format(jar.jarFormat, version);
        }

        public String codeSha256() throws NoSuchAlgorithmException, IOException {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream is = Files.newInputStream(jarsDirectory.resolve(fileName()))) {
                DigestInputStream digestStream = new DigestInputStream(is, digest);
                while (digestStream.read() != -1) {
                    // Consume stream
                }
            }
            return Base64.getEncoder().encodeToString(digest.digest());
        }
    }

    public static final class Jar {
        private final String jarFormat;

        public Jar(String jarFormat) {
            this.jarFormat = jarFormat;
        }
    }

}
