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
package sleeper.core.deploy;

import sleeper.core.SleeperVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Definitions of jar files used to deploy client functions.
 */
public class ClientJar {

    private static final List<ClientJar> ALL = new ArrayList<>();

    public static final ClientJar BULK_IMPORT_RUNNER = withFormatAndImage("bulk-import-runner-%s.jar", "bulk-import-runner");
    public static final ClientJar CDK = withFormatAndImage("cdk-%s.jar", "cdk");
    public static final ClientJar CLIENTS_UTILITY = withFormatAndImage("clients-%s-utility.jar", "clients-utility");

    private final String filename;
    private final String imageName;

    private ClientJar(String filename, String version, String imageName) {
        this.filename = Objects.requireNonNull(filename, "filename must not be null");
        this.imageName = Objects.requireNonNull(imageName, "imageName must not be null");
    }

    /**
     * Creates a jar definition with a filename computed by adding the Sleeper version to the given format string.
     *
     * @param  format    the format string
     * @param  imageName the name of the Docker image built from this jar
     * @return           the jar definition
     */
    public static ClientJar withFormatAndImage(String format, String imageName) {
        ClientJar jar = new ClientJar(format, SleeperVersion.getVersion(), imageName);
        ALL.add(jar);
        return jar;
    }

    public String getFilename() {
        return filename;
    }

    public String getImageName() {
        return imageName;
    }

    @Override
    public String toString() {
        return "ClientJar{filename=" + filename + ", imageName=" + imageName + "}";
    }

    public static List<ClientJar> getAll() {
        return Collections.unmodifiableList(ALL);
    }
}
